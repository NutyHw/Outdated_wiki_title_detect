import tweepy
from datetime import datetime, timedelta
import json
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import threading
from copy import deepcopy
from dateutil.parser import parse
from bson.objectid import ObjectId

load_dotenv(dotenv_path='../config/crawler.env')

class TwitterCrawler:
    def __init__(self, mode, threshold):
        self.processTweetsIds = set()
        self.processUserIds = set()

        self.taskPool = list()
        self.taskQueue = list()

        self.processTweetsIds = set()
        self.processUserIds = set()

        self.taskQueue = list()
        self.taskPool = list()

        self.queueUserIds = set()
        self.usersRecords = list()
        self.tweetsRecord = list()

        self.apis = list()

        self.processTweetsIdsLocker = threading.RLock()
        self.processUserIdsLocker = threading.RLock()
        self.queueUserIdsLocker = threading.RLock()
        self.usersRecordsLocker = threading.RLock()
        self.tweetsRecordLocker = threading.RLock()
        self.apisLocker = threading.RLock()
        self.taskPoolLocker = threading.RLock()

        self.threadThreshold = threshold
        self.authenApis('../config/app.json')
        if mode == 'start':
            self.initTask()
        elif mode == 'continue':
            self.loadTask()

    def connect(self):
        client = MongoClient(
            host=os.getenv('host'),
            port=int(os.getenv('port')),
            username=os.getenv('username'),
            password=os.getenv('password'),
            authSource=os.getenv('authSource'),
            authMechanism=os.getenv('authMechanism')
        )

        db = client[os.getenv('authSource')]
        return db

    def loadState(self):
        db = self.connect()
        db.drop_collection('taskPool')

    def saveState(self):
        db = self.connect()

        with self.tweetsRecordLocker:
            deleteRecord = list()
            for i in range(len(self.tweetsRecord)):
                record = self.tweetsRecord[i]
                result = db.rawTweets.update_one(
                    { 'id' : record['id'] },
                    { '$push' : { 'entities' : record['entities'][0] }}
                )
                if result.matched_count > 0:
                    deleteRecord.append(i)
            self.tweetsRecord = [ self.tweetsRecord[i] for i in range(len(self.tweetsRecord)) if i not in deleteRecord ]
            db.rawTweets.insert_many(self.tweetsRecord)
            self.tweetsRecord.clear()

        with self.usersRecordsLocker:
            deleteRecord = list()
            for i in range(len(self.usersRecords)):
                record = self.usersRecords[i]
                result = db.rawUsers.update_one(
                    { 'id' : record['id'] },
                    { '$push' : { 'entities' : record['entities'][0] }}
                )
                if result.matched_count > 0:
                    deleteRecord.append(i)
            self.usersRecords = [ self.usersRecords[i] for i in range(len(self.usersRecords)) if i not in deleteRecord ]
            db.rawUsers.insert_many(self.usersRecords)
            self.usersRecords.clear()

        rawTweetsCount = db.rawTweets.find().count()
        rawUsersCount = db.rawUsers.find().count()
        with open('crawler.log','a') as f:
            f.writelines('Tweets count {rawTweetsCount}, Users count {rawUsersCount}\n')

    def saveTask(self):
        db = self.connect()

        with self.taskPoolLocker:
            db.taskPool.insert_many(self.taskPool)
            self.taskPool.clear()

    def loadTask(self):
        db = self.connect()
        cursor = db.taskPool.find()

        for record in cursor:
            if len(self.taskQueue) > 10000:
                break
            self.taskQueue.append(record)

        db.taskPool.delete_many({ '_id' : { '$in' : [ ObjectId(task['_id']) for task in self.taskQueue ] } })

        if len(self.taskQueue) < 10000:
            if len(self.taskPool) < 10000:
                self.taskQueue = deepcopy(self.taskPool)
                self.taskPool.clear()
            else:
                self.taskQueue = deepcopy(self.taskPool[:10000])
                self.taskPool = self.taskPool[10000:]


    def createTasks(self,**kwargs):
        with self.taskPoolLocker:
            if kwargs['function'] == 'searchTweet':
                self.taskPool.append({
                    'function' : 'searchTweet',
                    'kwargs' : {
                        'maxId' : kwargs['maxId'],
                        'mention' : kwargs['mention']
                    }
                })
            elif kwargs['function'] == 'followerList':
                self.taskPool.append({
                    'function' : 'followerList',
                    'kwargs' : {
                        'cursor' : kwargs['cursor'],
                        'userId' : kwargs['userId']
                    }
                })
            elif kwargs['function'] == 'retrieveTimelineStatus':
                self.taskPool.append({
                    'function' : 'retrieveTimelineStatus',
                    'kwargs' : {
                        'userId' : kwargs['userId'],
                        'maxId' : kwargs['maxId']
                }})

    def authenApis(self,fpath):
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

        with self.apisLocker:
            self.apis = authApis
            for api in self.apis:
                self.checkRateLimit(api)

    def checkRateLimit(self,api):
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

    def searchTweet(self,mention,api, maxId = -1):
        api['searchTweetLock'] = True
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

                    originalRecord = None

                    with self.processTweetsIdsLocker:
                        if tweet['id'] in self.processTweetsIds:
                            continue

                    if 'retweeted_status' in tweet.keys():
                        originalTweet = tweet['retweeted_status']
                        originalRecord = {
                            'created_at' : parse(originalTweet['created_at']),
                            'user_id' : originalTweet['user']['id'],
                            'id' : originalTweet['id'],
                            'full_text' : originalTweet['full_text'],
                            'hashtags' : originalTweet['entities']['hashtags'],
                            'user_mentions' : originalTweet['entities']['user_mentions'],
                            'entities' : [{
                                'updated_time' : datetime.now(),
                                'retweet_count' : originalTweet['retweet_count'],
                                'favorite_count' : originalTweet['favorite_count'],
                            }]
                        }

                    with self.tweetsRecordLocker:
                        record = {
                            'created_at' : parse(tweet['created_at']),
                            'user_id' : tweet['user']['id'],
                            'id' : tweet['id'],
                            'full_text' : tweet['full_text'],
                            'hashtags' : tweet['entities']['hashtags'],
                            'user_mentions' : tweet['entities']['user_mentions'],
                            'entities' : [ {
                                'updated_time' : datetime.now(),
                                'retweet_count' : tweet['retweet_count'],
                                'favorite_count' : tweet['favorite_count'],
                            }]
                        }

                        with self.processTweetsIdsLocker:
                            if tweet['id'] not in self.processTweetsIds:
                                self.processTweetsIds = self.processTweetsIds.union({ tweet['id'] })
                                if originalRecord is None:
                                    self.tweetsRecord.append(record)
                                else:
                                    self.tweetsRecord.append({
                                        'record' : tweet['created_at'],
                                        'user_id' : tweet['user']['id'],
                                        'id' : tweet['id'],
                                        'entities' : [ {
                                            'updated_time' : datetime.now(),
                                            'retweet_count' : tweet['retweet_count'],
                                            'favorite_count' : tweet['favorite_count'],
                                        } ],
                                        'retweet_from' : originalRecord['id']
                                    })
                                    if originalRecord['id'] not in self.processTweetsIds:
                                        self.processTweetsIds = self.processTweetsIds.union({ originalRecord['id'] })
                                        self.tweetsRecord.append(originalRecord)

                    if tweet['id'] < maxId or maxId == -1:
                        isExhaust = False
                        maxId = tweet['id']

                    with self.usersRecordsLocker:
                        with self.processUserIdsLocker:
                            if tweet['user']['id'] not in self.processUserIds:
                                self.queueUserIds = self.queueUserIds.union({ tweet['user']['id'] })
                                self.processUserIds = self.processUserIds.union({ tweet['user']['id'] })
                                self.usersRecords.append({
                                    'id' : tweet['user']['id'],
                                    'name' : tweet['user']['name'],
                                    'screen_name' : tweet['user']['screen_name'],
                                    'location' : tweet['user']['location'],
                                    'entities' : [ {
                                        'updated_time' : datetime.now(),
                                        'followers_count' : tweet['user']['followers_count'],
                                        'friends_count' : tweet['user']['friends_count'],
                                        'statuses_count' : tweet['user']['statuses_count'],
                                    } ],
                                    'created_at' : parse(tweet['user']['created_at'])
                                })

                            if 'retweeted_status' in tweet.keys() and tweet['retweeted_status']['user']['id'] not in self.processUserIds:
                                self.queueUserIds = self.queueUserIds.union({ tweet['retweeted_status']['user']['id'] })
                                self.processUserIds = self.processUserIds.union({ tweet['retweeted_status']['user']['id'] })
                                self.usersRecords.append({
                                    'id' : originalTweet['user']['id'],
                                    'name' : originalTweet['user']['name'],
                                    'screen_name' : originalTweet['user']['screen_name'],
                                    'location' : originalTweet['user']['location'],
                                    'entities' : [ {
                                        'updated_time' : datetime.now(),
                                        'followers_count' : originalTweet['user']['followers_count'],
                                        'friends_count' : originalTweet['user']['friends_count'],
                                        'statuses_count' : originalTweet['user']['statuses_count'],
                                    } ],
                                    'created_at' : parse(originalTweet['user']['created_at'])
                                })

                api['searchRequestLeft'] -= 1

        except tweepy.RateLimitError:
            self.checkRateLimit(api)
        except tweepy.TweepError:
            pass

        if not isExhaust:
            self.createTasks(function='searchTweet', maxId=maxId, mention=mention)

        api['searchTweetLock'] = False

    def retrieveTimelineStatus(self,userId, api, maxId=-1):
        api['userTimelineLock'] = True
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

                    with self.processTweetsIdsLocker:
                        if tweet['id'] in self.processTweetsIds:
                            continue

                    originalRecord = None

                    if 'retweeted_status' in tweet.keys():
                        originalTweet = tweet['retweeted_status']
                        originalRecord = {
                            'created_at' : parse(originalTweet['created_at']),
                            'user_id' : originalTweet['user']['id'],
                            'id' : originalTweet['id'],
                            'full_text' : originalTweet['full_text'],
                            'hashtags' : originalTweet['entities']['hashtags'],
                            'user_mentions' : originalTweet['entities']['user_mentions'],
                            'entities' : [ {
                                'updated_time' : datetime.now(),
                                'retweet_count' : originalTweet['retweet_count'],
                                'favorite_count' : originalTweet['favorite_count'],
                            } ]
                        }

                    with self.tweetsRecordLocker:
                        record = {
                            'created_at' : parse(tweet['created_at']),
                            'user_id' : tweet['user']['id'],
                            'id' : tweet['id'],
                            'full_text' : tweet['full_text'],
                            'hashtags' : tweet['entities']['hashtags'],
                            'user_mentions' : tweet['entities']['user_mentions'],
                            'entities' : [ {
                                'updated_time' : datetime.now(),
                                'retweet_count' : tweet['retweet_count'],
                                'favorite_count' : tweet['favorite_count'],
                            } ]
                        }

                        with self.processTweetsIdsLocker:
                            if tweet['id'] not in self.processTweetsIds:
                                self.processTweetsIds = self.processTweetsIds.union({ tweet['id'] })
                                if originalRecord is None:
                                    self.tweetsRecord.append(record)
                                else:
                                    self.tweetsRecord.append({
                                        'record' : tweet['created_at'],
                                        'user_id' : tweet['user']['id'],
                                        'entities' : [ {
                                            'updated_time' : datetime.now(),
                                            'retweet_count' : tweet['retweet_count'],
                                            'favorite_count' : tweet['favorite_count'],
                                        }],
                                        'retweet_from' : originalRecord['id']
                                    })
                                    if originalRecord['id'] not in self.processTweetsIds:
                                        self.tweetsRecord.append(originalRecord)
                                        self.processTweetsIds = self.processTweetsIds.union({ originalRecord['id'] })


                    if tweet['id'] < maxId or maxId == -1:
                        maxId = tweet['id']
                        isExhaust = False

                api['userTimelineLeft'] -= 1
        except tweepy.RateLimitError:
            self.checkRateLimit(api)
        except tweepy.TweepError:
            pass

        if not isExhaust:
            self.createTasks(userId=userId, maxId=maxId, function='retrieveTimelineStatus')
        api['userTimelineLock'] = False

    def followerList(self, userId, api, cursor=-1):
        api['followerLock'] = True
        try:
            while cursor != 0 and api['followerRequestLeft'] > 0:
                response = api['api'].followers(id = userId, cursor=cursor, count = 200)

                for user in response[0]:
                    user = user._json

                    with self.processUserIdsLocker:
                        if user['id'] in self.processUserIds:
                            continue
                        self.processUserIds = self.processUserIds.union({ user['id'] })

                    with self.queueUserIdsLocker:
                        self.queueUserIds = self.queueUserIds.union({user['id']})

                    with self.usersRecordsLocker:
                        self.usersRecords.append({
                            'id' : user['id'],
                            'name' : user['name'],
                            'screen_name' : user['screen_name'],
                            'location' : user['location'],
                            'followers_count' : user['followers_count'],
                            'follow' : userId,
                            'entities' : [
                                {
                                    'updated_time' : datetime.now(),
                                    'followers_count' : user['followers_count'],
                                    'friends_count' : user['friends_count'],
                                    'statuses_count' : user['statuses_count']
                                } 
                            ]
                        })

                cursor = response[1][1]
        except tweepy.RateLimitError:
            self.checkRateLimit(api)
        except tweepy.TweepError:
            pass

        if cursor != 0:
            self.createTasks(userId=userId, cursor=cursor,function='followerList')
        api['followerLock'] = False

    def initTask(self):
        for name in screenNames:
            self.taskQueue.append({
                'function' : 'searchTweet',
                'kwargs' : {
                    'mention' : name,
                    'maxId' : -1
                }
            })

    def run(self):
        lastCheckRatelimit = datetime.now()
        lastSave = datetime.now()
        runUntil = datetime.now() + timedelta(hours=12)

        allThreds = list()
        while datetime.now() < runUntil and len(self.taskQueue) > 0:
            deleteTask = list()

            if threading.activeCount() > self.threadThreshold:
                for thread in allThreds:
                    thread.join()
                allThreds.clear()

            if lastSave + timedelta(hours=1) < datetime.now():
                thread = threading.Thread(target=self.saveState)
                thread.start()
                allThreds.append(thread)
                lastSave = datetime.now()

            with self.queueUserIdsLocker:
                for userId in self.queueUserIds:
                    self.createTasks(function='followerList', cursor=-1, userId=userId)
                    self.createTasks(function='retrieveTimelineStatus', maxId=-1, userId=userId)
                self.queueUserIds.clear()

            for i in range(len(self.taskQueue)):
                if threading.activeCount() > self.threadThreshold:
                    for thread in allThreds:
                        thread.join()
                    allThreds.clear()

                task = deepcopy(self.taskQueue[i])
                for api in self.apis:
                    if task['function'] == 'searchTweet' and not api['searchTweetLock']:
                        if api['searchRequestLeft'] > 0:
                            task['kwargs']['api'] = api
                            thread = threading.Thread(target=self.searchTweet, kwargs=task['kwargs'])
                            thread.start()
                            allThreds.append(thread)
                            deleteTask.append(self.taskQueue[i])
                            break

                    elif task['function'] == 'followerList' and not api['followerLock']:
                        if api['followerRequestLeft'] > 0:
                            task['kwargs']['api'] = api
                            thread = threading.Thread(target=self.followerList, kwargs=task['kwargs'])
                            thread.start()
                            deleteTask.append(self.taskQueue[i])
                            break

                    elif task['function'] == 'retrieveTimelineStatus' and not api['userTimelineLock']:
                        if api['userTimelineLeft'] > 0:
                            task['kwargs']['api'] = api
                            thread = threading.Thread(target=self.retrieveTimelineStatus, kwargs=task['kwargs'])
                            thread.start()
                            deleteTask.append(self.taskQueue[i])
                            break

            for task in deleteTask:
                self.taskQueue.remove(task)
            deleteTask.clear()

            if len(self.taskPool) > 10000:
                self.saveTask()

            if len(self.taskQueue) == 0:
                self.loadTask()

if __name__ == '__main__':
    screenNames = list()
    with open('../data/twitter_seed.txt') as f:
        screenNames = f.read().splitlines()
    crawler = TwitterCrawler(mode='start', threshold=8)
    crawler.saveState()
