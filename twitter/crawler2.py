import tweepy
from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import threading
from copy import deepcopy
from dateutil.parser import parse

load_dotenv(dotenv_path='../config/crawler.env')

processTweetsIds = set()
processUserIds = set()

taskQueue = list()
taskPool = list()

queueUserIds = set()
usersRecords = list()
tweetsRecord = list()
apis = list()

Locks = {
    'processTweetsIds' : threading.RLock(),
    'processUserIds' : threading.RLock(),
    'queueUserIds' : threading.RLock(),
    'usersRecords' : threading.RLock(),
    'tweetsRecord' : threading.RLock(),
    'apis' : threading.RLock(),
    'taskPool' : threading.RLock()
}

def loadState():
    global processTweetsIds
    global processUserIds

    client = MongoClient(
        host=os.getenv('host'),
        port=int(os.getenv('port')),
        username=os.getenv('username'),
        password=os.getenv('password'),
        authSource=os.getenv('authSource'),
        authMechanism=os.getenv('authMechanism')
    )

    db = client[os.getenv('authSource')]
    processTweetsIds = set(db.preprocessTweets.distinct('id'))
    processUserIds = set(db.preprocessUsers.distinct('id'))

def saveState():
    global processTweetsIds
    global processUserIds
    global tweetsRecord
    global usersRecords
    global Locks

    client = MongoClient(
        host=os.getenv('host'),
        port=int(os.getenv('port')),
        username=os.getenv('username'),
        password=os.getenv('password'),
        authSource=os.getenv('authSource'),
        authMechanism=os.getenv('authMechanism')
    )

    db = client[os.getenv('authSource')]

    with Locks['tweetsRecord']:
        with Locks['usersRecords']:
            with open('crawler.log','a') as f:
                f.write(f'tweetsRecord : {len(tweetsRecord)}, usersRecords : {len(usersRecords)}\n')

            if len(usersRecords) > 0:
                db.rawUsers.insert_many(usersRecords)
            if len(tweetsRecord) > 0:
                db.rawTweets.insert_many(tweetsRecord)

            usersRecords.clear()
            tweetsRecord.clear()

    with Locks['taskQueue']:
        with open('taskQueue.txt','w') as f:
            json.dump(taskQueue,f)

    with Locks['processTweetsIds']:
        with open('processTweetsIds.txt','w') as f:
            json.dump(list(processTweetsIds),f)

    with Locks['processUserIds']:
        with open('processUserIds.txt','w') as f:
            json.dump(list(processUserIds),f)

def saveTask():
    client = MongoClient(
        host=os.getenv('host'),
        port=int(os.getenv('port')),
        username=os.getenv('username'),
        password=os.getenv('password'),
        authSource=os.getenv('authSource'),
        authMechanism=os.getenv('authMechanism')
    )

    db = client[os.getenv('authSource')]

    with Locks['processPool']:
        db.taskPool.insert_many(processPool)
        taskPool.clear()

def loadTask():
    client = MongoClient(
        host=os.getenv('host'),
        port=int(os.getenv('port')),
        username=os.getenv('username'),
        password=os.getenv('password'),
        authSource=os.getenv('authSource'),
        authMechanism=os.getenv('authMechanism')
    )

    db = client[os.getenv('authSource')]
    cursor = db.taskPool.find()

    for record in cursor:
        if len(taskQueue) > 10000:
            break
        taskQueue.append(record)

    db.taskPool.delete_many({ '_id' : { '$in' : [ task['_id'] for task in taskQueue ] } })

def createTasks(**kwargs):
    global taskPool

    with Locks['taskPool']:
        if kwargs['function'] == 'searchTweet':
            taskPool.append({
                'function' : 'searchTweet',
                'kwargs' : {
                    'maxId' : kwargs['maxId'],
                    'mention' : kwargs['mention']
                }
            })
        elif kwargs['function'] == 'followerList':
            taskPool.append({
                'function' : 'followerList',
                'kwargs' : {
                    'cursor' : kwargs['cursor'],
                    'userId' : kwargs['userId']
                }
            })
        elif kwargs['function'] == 'retrieveTimelineStatus':
            taskPool.append({
            'function' : 'retrieveTimelineStatus',
            'kwargs' : {
                'userId' : kwargs['userId'],
                'maxId' : kwargs['maxId']
            }})

def authenApis(fpath):
    global apis
    config = None

    authApis = list()

    with open(fpath) as f:
        config = json.load(f)

    for app in config:
        auth = tweepy.OAuthHandler(config[app]['API_KEY'],config[app]['API_SECRET'])
        auth.set_access_token(config[app]['ACCESS_TOKEN'],config[app]['ACCESS_SECRET'])
        authApis.append({
            'api' : tweepy.API(auth),
            'searchResetTime' : None,
            'searchRequestLeft' : None,
            'searchTweetLock' : False,
            'followerResetTime' : None,
            'followerRequestLeft' : None, 
            'followerLock' : False,
            'userTimelineResetTime' : None,
            'userTimelineLeft' : None,
            'userTimelineLock' : False
        })

    with Locks['apis']:
        apis = authApis

    for api in apis:
        checkRateLimit(api)

def checkRateLimit(api):
    with api['mutexLock']:
        response = api['api'].rate_limit_status()
        search = response['resources']['search']['/search/tweets']
        api['searchRequestLeft'] = search['remaining']
        api['searchResetTime'] = search['reset']

        follower = response['resources']['followers']['/followers/list']
        api['followerRequestLeft'] = follower['remaining']
        api['followerResetTime'] = follower['reset']


        timeline = response['resources']['statuses']['/statuses/user_timeline']
        api['userTimelineLeft'] = timeline['remaining']
        api['userTimelineResetTime'] = timeline['reset']

def searchTweet(mention,api, maxId = -1):
    global processTweetsIds
    global processUserIds
    global tweetsRecord
    global usersRecords
    global queueUserIds

    tweets = list()
    users = list()
    isExhaust = False

    try:
        while api['searchRequestLeft'] > 0 and not isExhaust:
            if maxId == -1:
                response = api['api'].search(q=mention, lang='th', count = 100, result_type = 'mixed', tweet_mode='extended')
            else:
                response = api['api'].search(q=mention, lang='th', count = 100, result_type = 'mixed', max_id = maxId, tweet_mode='extended')

            isExhaust = True

            for tweet in response:
                tweet = tweet._json

                with Locks['processTweetsIds']:
                    if tweet['id'] in processTweetsIds:
                        continue
                    processTweetsIds = processTweetsIds.union({tweet['id']})

                record = {
                    'created_at' : parse(tweet['created_at']),
                    'user_id' : tweet['user']['id'],
                    'id' : tweet['id'],
                    'full_text' : tweet['full_text'],
                    'hashtags' : tweet['entities']['hashtags'],
                    'user_mentions' : tweet['entities']['user_mentions'],
                    'retweet_count' : tweet['retweet_count'],
                    'retweeted' : 'retweeted_status' in tweet.keys()
                }

                if 'retweeted_status' in tweet.keys():
                    record['full_text'] = tweet['retweeted_status']['full_text']

                with Locks['tweetsRecord']:
                    tweetsRecord.append(record)

                if tweet['id'] < maxId or maxId == -1:
                    isExhaust = False
                    maxId = tweet['id']

                with Locks['processUserIds']:
                    if tweet['user']['id'] in processUserIds:
                        continue
                    processUserIds = processUserIds.union(tweet['user']['id'])

                with Locks['usersRecords']:
                    usersRecords.append({
                        'id' : tweet['user']['id'],
                        'name' : tweet['user']['name'],
                        'screen_name' : tweet['user']['screen_name'],
                        'location' : tweet['user']['location'],
                        'followers_count' : tweet['user']['followers_count'],
                        'friends_count' : tweet['user']['friends_count'],
                        'statuses_count' : tweet['user']['statuses_count'],
                        'created_at' : parse(tweet['user']['created_at'])
                    })

                with Locks['queueUserIds']:
                    queueUserIds = queueUserIds.union({tweet['user']['id']})

            api['searchRequestLeft'] -= 1
    except tweepy.RateLimitError:
        checkRateLimit(api)
    except tweepy.TweepError:
        pass

    if not isExhaust:
        createTasks(function='searchTweet', maxId=maxId, mention=mention)

    api['searchTweetLock'] = False

def retrieveTimelineStatus(userId, api, maxId=-1):
    global tweetsRecord
    global processTweetsIds

    isExhaust = False

    try:
        while api['userTimelineLeft'] > 0 and not isExhaust:
            response = None

            if maxId == -1:
                response = api['api'].user_timeline(id=userId, count=200, tweet_mode='extended')
            else:
                response = api['api'].user_timeline(id=userId, max_id=maxId, count=200, tweet_mode='extended')

            isExhaust = True
            for tweet in response:
                tweet = tweet._json

                with Locks['processTweetsIds']:
                    if tweet['id'] in processTweetsIds:
                        continue
                    processTweetsIds = processTweetsIds.union({tweet['id']})

                record = {
                    'created_at' : parse(tweet['created_at']),
                    'user_id' : tweet['user']['id'],
                    'id' : tweet['id'],
                    'full_text' : tweet['full_text'],
                    'hashtags' : tweet['entities']['hashtags'],
                    'user_mentions' : tweet['entities']['user_mentions'],
                    'retweet_count' : tweet['retweet_count'],
                    'retweeted' : 'retweeted_status' in tweet.keys()
                }

                if 'retweeted_status' in tweet:
                    record['full_text'] = tweet['retweeted_status']['full_text']

                with Locks['tweetsRecord']:
                    tweetsRecord.append(record)

                if tweet['id'] < maxId or maxId == -1:
                    maxId = tweet['id']
                    isExhaust = False

            api['userTimelineLeft'] -= 1
    except tweepy.RateLimitError:
        checkRateLimit(api)
    except tweepy.TweepError:
        pass

    if not isExhaust:
        createTasks(userId=userId, maxId=maxId, function='retrieveTimelineStatus')
    with api['mutexLock']:
        api['userTimelineLock'] = False

def followerList(userId, api, cursor=-1):
    global processUserIds
    global queueUserIds
    try:
        while cursor != 0 and api['followerRequestLeft'] > 0:
            response = api['api'].followers(id = userId, cursor=cursor, count = 200)

            for user in response[0]:
                user = user._json

                with Locks['processUserIds']:
                    if user['id'] in processUserIds:
                        continue
                    processUserIds = processUserIds.union({user['id']})

                with Locks['queueUserIds']:
                    queueUserIds = queueUserIds.union({user['id']})

                with Locks['usersRecords']:
                    usersRecords.append({
                        'id' : user['id'],
                        'name' : user['name'],
                        'screen_name' : user['screen_name'],
                        'location' : user['location'],
                        'followers_count' : user['followers_count'],
                        'friends_count' : user['friends_count'],
                        'statuses_count' : user['statuses_count'],
                        'created_at' : parse(user['created_at']),
                        'follow' : str(userId)
                    })

            cursor = response[1][1]
    except tweepy.RateLimitError:
        checkRateLimit(api)
    except tweepy.TweepError:
        pass

    if cursor != 0:
        createTasks(userId=userId, cursor=cursor,function='followerList')
    with api['mutexLock']:
        api['followerLock'] = False

def initTask():
    global taskQueue
    for name in screenNames:
        taskQueue.append({
            'function' : 'searchTweet',
            'kwargs' : {
                'mention' : name,
                'maxId' : -1
            }
        })

def scheduler():
    global taskQueue
    global apis
    global queueUserIds

    lastCheckRatelimit = datetime.now()
    lastSave = datetime.now()
    runUntil = datetime.now() + timedelta(days=1)

    while datetime.now() < runUntil and len(taskQueue) > 0:
        deleteTask = list()

        if threading.activeCount() > 8:
            continue

        if lastSave + timedelta(hours=1) < datetime.now():
            thread = threading.Thread(target=saveState)
            thread.start()
            lastSave = datetime.now()

        if lastCheckRatelimit + timedelta(minutes=5) < datetime.now():
            thread = threading.Thread(target=authenApis, args=('../config/app.json',))
            thread.start()
            lastCheckRatelimit = datetime.now()

        for userId in queueUserIds:
            createTasks(function='followerList', cursor=-1, userId=userId)
            createTasks(function='retrieveTimelineStatus', maxId=-1, userId=userId)

        for i in range(len(taskQueue)):
            if threading.activeCount() > 3:
                break

            task = deepcopy(taskQueue[i])
            for api in apis:
                if task['function'] == 'searchTweet' and not api['searchTweetLock']:
                    if api['searchRequestLeft'] > 0:
                        task['kwargs']['api'] = api
                        thread = threading.Thread(target=searchTweet, kwargs=task['kwargs'])
                        thread.start()
                        api['searchTweetLock'] = True
                        deleteTask.append(taskQueue[i])
                        break

                    elif task['function'] == 'followerList' and not api['followerLock']:
                        if api['followerRequestLeft'] > 0:
                            task['kwargs']['api'] = api
                            thread = threading.Thread(target=followerList, kwargs=task['kwargs'])
                            thread.start()
                            api['followerLock'] = True
                            deleteTask.append(taskQueue[i])
                            break

                    elif task['function'] == 'retrieveTimelineStatus' and not api['userTimelineLock']:
                        if api['userTimelineLeft'] > 0:
                            task['kwargs']['api'] = api
                            thread = threading.Thread(target=retrieveTimelineStatus, kwargs=task['kwargs'])
                            thread.start()
                            api['userTimelineLock'] = True
                            deleteTask.append(taskQueue[i])
                            break

        for task in deleteTask:
            taskQueue.remove(task)
        deleteTask.clear()

        if len(taskQueue) == 0:
            loadProcess()

        for i in range(len(taskPool)):
            if len(taskQueue) < 10000:
                taskQueue.append(deepcopy(taskPool[i]))
            else:
                taskPool = taskPool[i:]
                break

        if len(taskPool) > 10000:
            saveProcess()

if __name__ == '__main__':
    screenNames = list()
    with open('../data/twitter_seed.txt') as f:
        screenNames = f.read().splitlines()

    authenApis('../config/app.json')
    initTask()
    scheduler()
