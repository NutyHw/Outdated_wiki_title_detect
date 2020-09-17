from pymongo import MongoClient
from dateutil.parser import parse
from datetime import timedelta
import os
import json
from dotenv import load_dotenv

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
    return client.twitter_event_detection

def collectionDuplicater(db,colName):
    records = list(db[colName].find())
    for record in records:
        record['created_at'] = parse(record['created_at'])
    db['duplicated_'+colName].insert_many(records)

def createTweetsTime(db):
    pipeline = [ 
        { '$sort' : { 'created_at' : 1 } }
    ]

    records = list(db.duplicated_rawTweets.aggregate(pipeline, allowDiskUse=True))
    startTime = records[0]['created_at']
    counts = dict()
    counts[startTime.strftime("%Y-%m-%d %H:%M:%S")] = 0
    for record in records:
        if startTime + timedelta(hours=3) > record['created_at'] and record['created_at'] >= startTime:
            counts[startTime.strftime("%Y-%m-%d %H:%M:%S")] += 1
        else:
            startTime += timedelta(hours=3)
            counts[startTime.strftime("%Y-%m-%d %H:%M:%S")] = 1
    return counts

def removeDuplicateTweets(db):
    cursor = db.rawTweets.find({})
    
    preprocessRecords = list()
    ids = set()
    for record in cursor:
        if record['id'] in ids:
            continue
        ids = ids.union({record['id']})
        record['created_at'] = parse(record['created_at'])
        preprocessRecords.append(record)

    db.preprocessTweets.insert_many(preprocessRecords)

def removeDuplicateUsers(db):
    cursor = db.rawUsers.find({})

    preprocessUsers = list()
    ids = set()

    for record in cursor:
        if record['id'] in ids:
            continue
        ids = ids.union({record['id']})
        record['created_at'] = parse(record['created_at'])
        preprocessUsers.append(record)
    db.preprocessUsers.insert_many(preprocessUsers)

if __name__ == '__main__':
    db = connect()
    removeDuplicateUsers(db)
    removeDuplicateTweets(db)
