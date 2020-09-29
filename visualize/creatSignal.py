from pymongo import MongoClient
from datetime import timedelta, datetime
from dotenv import load_dotenv
import json
import os

load_dotenv('../config/crawler.env')

def connect():
    client = MongoClient(
        host=os.getenv('host'),
        port=int(os.getenv('port')),
        username=os.getenv('username'),
        password=os.getenv('password'),
        authSource=os.getenv('authSource'),
        authMechanism=os.getenv('authMechanism')
    )

    return client[os.getenv('authSource')]

def getTweetsTimestamp(db):
    pipelines = [ 
        { '$unwind' : '$hashtags' },
        { '$project' : { 'created_at' : '$created_at', 'hashtag' : '$hashtags.text' }},
        { '$match' : {  'created_at' : { '$gte' : datetime(2020,1,1) } }},
        { '$sort' : { 'created_at' : 1 }},
        { '$group' : {
            '_id' : { 'hashtag' : '$hashtag' },
            'created_at' : { '$push' : '$created_at' }
        }},
        { '$project' : { 'hashtag' : '$_id.hashtag', 'created_at' : '$created_at', 'size' : { '$size' : '$created_at' }}},
        { '$sort' : { 'size' : -1 }},
        { '$limit' : 50 }
    ]
    cursor = db.rawTweets.aggregate(pipelines, allowDiskUse=True)

    return list(cursor)

def createTimewindowFreq(records):
    startTime = None
    endTime = None

    for record in records:
        if endTime is None or endTime < record['created_at'][-1]:
            endTime = record['created_at'][-1]
        if startTime is None or startTime > record['created_at'][0]:
            startTime = record['created_at'][0]

    curPos = dict()
    hashtagTimewindowFreq = dict()

    for record in records:
        curPos[record['hashtag']] = 0
        hashtagTimewindowFreq[record['hashtag']] = list()
        hashtagTimewindowFreq['created_at'] = list()

    curTime = startTime
    while curTime <= endTime:
        hashtagTimewindowFreq['created_at'].append(datetime.timestamp(curTime))
        for record in records:
            counter = 0
            for i in range(curPos[record['hashtag']],len(record['created_at'])):
                created_at = record['created_at'][i]
                if created_at >= curTime and created_at < curTime + timedelta(hours=3):
                    counter += 1
                    curPos[record['hashtag']] = i
            hashtagTimewindowFreq[record['hashtag']].append(counter)
        curTime += timedelta(hours=3)

    hashtagTimewindowFreq['created_at'].append(datetime.timestamp(curTime))
    return hashtagTimewindowFreq

if __name__ == '__main__':
    db = connect()
    records = getTweetsTimestamp(db)
    topHashtagFreq = createTimewindowFreq(records)

    with open('topHashtagFreq','w') as f:
        json.dump(topHashtagFreq,f)
