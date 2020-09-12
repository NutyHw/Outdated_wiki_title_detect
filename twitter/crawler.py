import tweepy
from datetime import datetime, timedelta
import json
import time
import sched
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='../config/crawler.env')

processTweetsIds = set()
processUserIds = set()

queueUserIds = set()

tweetsRecord = list()
usersRecords = list()

screenNames = list()
apis = list()


taskQueue = list()
processQueue = sched.scheduler()

def saveState():
    global processTweetsIds
    global processUserIds
    global taskQueue
    global queueUserIds
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

    # db.rawUsers.insert_many(usersRecords)
    # db.rawTweets.insert_many(tweetsRecord)

    usersRecords.clear()
    tweetsRecord.clear()

    with open('taskQueue.txt','w') as f:
        json.dump(taskQueue,f)
    with open('processTweetsIds.txt','w') as f:
        for id in processTweetsIds:
            f.writelines(str(id))
    with open('processUserIds.txt','w') as f:
        for id in processTweetsIds:
            f.writelines(str(id))
    with open('queueUserIds.txt','w') as f:
        for id in queueUserIds:
            f.writelines(str(id))

def createTasks(**kwargs):
    global processQueue

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

        
def checkRateLimit(api):
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

def authenApis(fpath):
    config = None
    apis = list()

    with open(fpath) as f:
        config = json.load(f)

    for app in config:
        auth = tweepy.OAuthHandler(config[app]['API_KEY'],config[app]['API_SECRET'])
        auth.set_access_token(config[app]['ACCESS_TOKEN'],config[app]['ACCESS_SECRET'])
        apis.append({
            'api' : tweepy.API(auth),
            'searchResetTime' : None,
            'searchRequestLeft' : None,
            'searchTweetOcupy' : False,
            'followerResetTime' : None,
            'followerRequestLeft' : None, 
            'followerListOcupy' : False,
            'userTimelineResetTime' : None,
            'userTimelineLeft' : None,
            'retrieveTimelineStatusOcupy' : False
        })

    return apis

def searchTweet(mention,api, maxId = -1):
    global processTweetsIds
    global processUserIds
    global queueUserIds
    global tweetsRecord
    global usersRecords

    tweets = list()
    users = list()
    isExhaust = False

    while api['searchRequestLeft'] > 0 and not isExhaust:
        isExhaust = True
        response = api['api'].search(q=mention, lang='th', count = 100, result_type = 'mixed', max_id = maxId, tweet_mode='extended')
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
                'retweeted' : tweet['retweeted']
            }

            if 'retweeted_status' in tweet:
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
    
    if not isExhaust:
        createTasks(function='searchTweet', maxId=maxId, mention=mention)

def retrieveTimelineStatus(userId, api, maxId=-1):
    global tweetsRecord
    global processTweetsIds

    isExhaust = False

    while api['userTimelineLeft'] > 0 and not isExhaust:
        isExhaust = True
        response = None

        if maxId == -1:
            response = api['api'].user_timeline(id=userId, count=200, tweet_mode='extended')
        else:
            response = api['api'].user_timeline(id=userId, max_id=maxId, count=200, tweet_mode='extended')

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
                'retweeted' : tweet['retweeted']
            }

            if 'retweeted_status' in tweet:
                record['full_text'] = tweet['retweeted_status']['full_text']
                
            tweetsRecord.append(record)

            if tweet['id'] < maxId:
                maxId = tweet['id']
                isExhaust = False

        api['userTimelineLeft'] -= 1

    if not isExhaust:
        createTasks(userId=userId, maxId=maxId, function='retrieveTimelineStatus')

def followerList(userId, api, cursor=-1):
    global processUserIds

    while cursor != 0 and api['followerRequestLeft'] > 0:
        response = api['api'].followers(id = userId, cursor=cursor, count = 200)
        for user in response[0]:
            user = user._json
            if user['id'] in processUserIds:
                continue

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
        cursor = response[1][1]

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
    global apis
    global processUserIds
    global processTweetsIds
    global queueUserIds
    global taskQueue
    global processQueue
    

    for api in apis:
        checkRateLimit(api)

    for userId in queueUserIds:
        createTasks(function='followerList', userId = userId, cursor=-1)
        createTasks(function='retrieveTimelineStatus', userId = userId, maxId = -1)

    processUserIds = processUserIds.union(set(queueUserIds))
    queueUserIds.clear()

    scheduleTask = list()
    deleteTask = list()

    for i in range(len(taskQueue)):
        minimumWaitingTime = time.time() * 2
        task = dict(taskQueue[i])
        chosenApi = None
        function = None
        for api in apis:
            if api[task['function']+'Ocupy']:
                continue

            if task['function'] == 'searchTweet':
                function = searchTweet
                if api['searchRequestLeft'] > 0:
                    minimumWaitingTime = time.time()
                    chosenApi = api

                elif api['searchResetTime'] < 'minimumWaitingTime':
                    minimumWaitingTime = api['searchResetTime']
                    chosenApi = api

            elif task['function'] == 'followerList':
                function = followerList
                if api['followerRequestLeft'] > 0:
                    minimumWaitingTime = time.time()
                    chosenApi = api

                elif api['followerResetTime'] < minimumWaitingTime:
                    minimumWaitingTime = api['followerResetTime']
                    chosenApi = api

            elif task['function'] == 'retrieveTimelineStatus':
                function = retrieveTimelineStatus
                if api['userTimelineLeft'] > 0:
                    minimumWaitingTime = time.time()
                    chosenApi = api

                elif api['userTimelineResetTime'] < minimumWaitingTime:
                    minimumWaitingTime = api['userTimelineResetTime']
                    chosenApi = api

        if chosenApi is None:
            continue

        chosenApi[task['function']+'Ocupy'] = True
        task['kwargs']['api'] = chosenApi
        task['startTime'] = minimumWaitingTime
        task['function'] = function

        scheduleTask.append(task)
        deleteTask.append(taskQueue[i])

    for task in deleteTask:
        taskQueue.remove(task)

    for task in scheduleTask:
        delay = task['startTime'] - time.time()
        processQueue.enter(delay=delay, priority=0, action= task['function'], kwargs=task['kwargs'])


if __name__ == '__main__':
    screenNames = list()
    apis = authenApis('../config/app.json')
    with open('../data/twitter_seed.txt') as f:
        screenNames = f.read().splitlines()

    lastSave = datetime.now()
    initTask()
    while len(processTweetsIds) < 1000000:
        scheduler()
        processQueue.run()
        if lastSave + timedelta(minutes=60) < datetime.now():
            with open('crawler.log','a') as f:
                f.writelines(f'userIds : {len(processUserIds)}, tweetIds : {len(processTweetsIds)}, tasks : {len(taskQueue)}')
            saveState()
            lastSave = datetime.now()


