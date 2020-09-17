import tweepy
from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import sched
import threading
from copy import deepcopy

load_dotenv(dotenv_path='../config/crawler.env')

processTweetsIds = set()
processUserIds = set()
taskQueue = list()
queueUserIds = set()

apis = list()

usersRecords = list()
tweetsRecord = list()

lock = threading.RLock()

def loadState():
    global processTweetsIds
    global processUserIds
    global taskQueue

    with open('taskQueue.txt') as f:
        taskQueue = json.load(f)
    with open('processTweetsIds') as f:
        processTweetsIds = json.load(f)
    with open('processUserIds.txt') as f:
        processUserIds = json.load(f)

def saveState():
    global processTweetsIds
    global processUserIds
    global tweetsRecord
    global usersRecords

    client = MongoClient(
        host=os.getenv('host'),
        port=int(os.getenv('port')),
        username=os.getenv('username'),
        password=os.getenv('password'),
        authSource=os.getenv('authSource'),
        authMechanism=os.getenv('authMechanism')
    )

    db = client[os.getenv('authSource')]

    lock.acquire()
    with open('crawler.log','a') as f:
        f.write(f'tweetsRecord : {len(tweetsRecord)}, usersRecords : {len(usersRecords)}\n')

    with open('taskQueue.txt','w') as f:
        json.dump(taskQueue,f)

    with open('processTweetsIds.txt','w') as f:
        json.dump(list(processTweetsIds),f)

    with open('processUserIds.txt','w') as f:
        json.dump(list(processUserIds),f)

    if len(usersRecords) > 0:
        db.rawUsers.insert_many(usersRecords)
    if len(tweetsRecord) > 0:
        db.rawTweets.insert_many(tweetsRecord)

    usersRecords.clear()
    tweetsRecord.clear()
    lock.release()

def createTasks(**kwargs):
    global taskQueue
    lock.acquire()
    if kwargs['function'] == 'searchTweet':
        taskQueue.append({
            'function' : 'searchTweet',
            'kwargs' : {
                'maxId' : kwargs['maxId'],
                'mention' : kwargs['mention']
            }
        })
    elif kwargs['function'] == 'followerList':
        taskQueue.append({
            'function' : 'followerList',
            'kwargs' : {
                'cursor' : kwargs['cursor'],
                'userId' : kwargs['userId']
            }
        })
    elif kwargs['function'] == 'retrieveTimelineStatus':
        taskQueue.append({
        'function' : 'retrieveTimelineStatus',
        'kwargs' : {
            'userId' : kwargs['userId'],
            'maxId' : kwargs['maxId']
        }})
    lock.release()

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
    lock.acquire()
    apis = authApis

    for api in apis:
        checkRateLimit(api)
    lock.release()

def checkRateLimit(api):
    lock.acquire()
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
    lock.release()

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
            lock.acquire()
            if maxId == -1:
                response = api['api'].search(q=mention, lang='th', count = 100, result_type = 'mixed', tweet_mode='extended')
            else:
                response = api['api'].search(q=mention, lang='th', count = 100, result_type = 'mixed', max_id = maxId, tweet_mode='extended')
            isExhaust = True

            for tweet in response:
                tweet = tweet._json
                if tweet['id'] in processTweetsIds:
                    continue

                record = {
                    'created_at' : tweet['created_at'],
                    'user_id' : tweet['user']['id'],
                    'id' : tweet['id'],
                    'full_text' : tweet['full_text'],
                    'hashtags' : tweet['entities']['hashtags'],
                    'user_mentions' : tweet['entities']['user_mentions'],
                    'retweet_count' : tweet['retweet_count'],
                    'retweeted' : hasattr(tweet, 'retweeted_status')
                }

                if 'retweeted_status' in tweet.keys():
                    record['full_text'] = tweet['retweeted_status']['full_text']

                tweetsRecord.append(record)
                processTweetsIds = processTweetsIds.union({tweet['id']})

                if tweet['id'] < maxId:
                    isExhaust = False
                    maxId = tweet['id']

                if tweet['user']['id'] in processUserIds:
                    continue

                usersRecords.append({
                    'id' : tweet['user']['id'],
                    'name' : tweet['user']['name'],
                    'screen_name' : tweet['user']['screen_name'],
                    'location' : tweet['user']['location'],
                    'followers_count' : tweet['user']['followers_count'],
                    'friends_count' : tweet['user']['friends_count'],
                    'statuses_count' : tweet['user']['statuses_count'],
                    'created_at' : tweet['user']['created_at']
                })

                queueUserIds = queueUserIds.union({tweet['user']['id']})

            api['searchRequestLeft'] -= 1
            lock.release()
    except tweepy.RateLimitError:
        checkRateLimit(api)
    except tweepy.TweepError:
        pass

    if not isExhaust:
        createTasks(function='searchTweet', maxId=maxId, mention=mention)

def retrieveTimelineStatus(userId, api, maxId=-1):
    global tweetsRecord
    global processTweetsIds

    isExhaust = False

    try:
        while api['userTimelineLeft'] > 0 and not isExhaust:
            lock.acquire()
            response = None

            if maxId == -1:
                response = api['api'].user_timeline(id=userId, count=200, tweet_mode='extended')
            else:
                response = api['api'].user_timeline(id=userId, max_id=maxId, count=200, tweet_mode='extended')

            isExhaust = True
            for tweet in response:
                tweet = tweet._json
                if tweet['id'] in processTweetsIds:
                    continue

                record = {
                    'created_at' : tweet['created_at'],
                    'user_id' : tweet['user']['id'],
                    'id' : tweet['id'],
                    'full_text' : tweet['full_text'],
                    'hashtags' : tweet['entities']['hashtags'],
                    'user_mentions' : tweet['entities']['user_mentions'],
                    'retweet_count' : tweet['retweet_count'],
                    'retweeted' : hasattr(tweet, 'retweeted_status')
                }

                if 'retweeted_status' in tweet:
                    record['full_text'] = tweet['retweeted_status']['full_text']

                tweetsRecord.append(record)

                if tweet['id'] < maxId:
                    maxId = tweet['id']
                    isExhaust = False

            api['userTimelineLeft'] -= 1
            lock.release()
    except tweepy.RateLimitError:
        checkRateLimit(api)
    except tweepy.TweepError:
        pass

    if not isExhaust:
        createTasks(userId=userId, maxId=maxId, function='retrieveTimelineStatus')

def followerList(userId, api, cursor=-1):
    global processUserIds
    global queueUserIds
    try:
        while cursor != 0 and api['followerRequestLeft'] > 0:
            lock.acquire()
            response = api['api'].followers(id = userId, cursor=cursor, count = 200)
            for user in response[0]:
                user = user._json
                if user['id'] in processUserIds:
                    continue
                queueUserIds = queueUserIds.union({user['id']})

                usersRecords.append({
                    'id' : user['id'],
                    'name' : user['name'],
                    'screen_name' : user['screen_name'],
                    'location' : user['location'],
                    'followers_count' : user['followers_count'],
                    'friends_count' : user['friends_count'],
                    'statuses_count' : user['statuses_count'],
                    'created_at' : user['created_at']
                })
            api['followerRequestLeft'] -= 1
            lock.release()
            cursor = response[1][1]
    except tweepy.RateLimitError:
        checkRateLimit(api)
    except tweepy.TweepError:
        pass

    if cursor != 0:
        createTasks(userId=userId, cursor=cursor,function='followerList')

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

    while len(taskQueue) > 0:
        deleteTask = list()

        if threading.activeCount() > 3:
            continue

        lock.acquire()

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
        lock.release()

if __name__ == '__main__':
    screenNames = list()
    with open('../data/twitter_seed.txt') as f:
        screenNames = f.read().splitlines()

    authenApis('../config/app.json')
    initTask()
    scheduler()
