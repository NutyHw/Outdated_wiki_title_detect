from pymongo import MongoClient
import seaborn as sns
import os
import pandas as pd
from dateutil.parser import parse
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv(dotenv_path='../config/crawler.env')

def connect():
    client = MongoClient(
        host=os.getenv('host'),
        port=int(os.getenv('port')),
        username=os.getenv('username'),
        password=os.getenv('password'),
        authSource=os.getenv('authSource'),
        authMechanism=os.getenv('authMechanism')
    )

    db = client.twitter_event_detection.rawTweetsDump
    return db

def getMentionRecord(fpath, db):
    res = dict()
    screenName = list()
    hashTags = set()
    with open(fpath) as f:
        screenName = f.read().splitlines()

    cursor = db.aggregate([
        { '$match' : { 'user_mentions.screen_name' : { '$in' : screenName } } }
    ])

    for record in cursor:
        hashTags = hashTags.union(set([ hashtag['text'] for hashtag in record['hashtags'] ]))
    
    cursor = db.aggregate([
        { '$match' : { 'hashtags.text' : { '$in' : list(hashTags) } } }
    ])

    for record in cursor:
        initTime = parse(record['created_at'])
        for hashtag in record['hashtags']:
            if hashtag['text'] in res.keys():
                res[hashtag['text']].append(initTime)
            else:
                res[hashtag['text']] = [ initTime ]

    for hashtag in res.keys():
        res[hashtag].sort()
    return res

if __name__ == '__main__':
    db = connect()
    res = getMentionRecord('../data/twitter_seed.txt',db)

    data = dict()
    
    startTime = None
    endTime = None
    for hashtag in res.keys():
        if not startTime or res[hashtag][0] < startTime:
            startTime = res[hashtag][0]
        if not endTime or res[hashtag][-1] > endTime:
            endTime = res[hashtag][-1]

    print(startTime)
    print(endTime)
    data['hashtags'] = list(res.keys())

    while startTime <= endTime:
        rows = list()
        for hashtag in res.keys():
            allTimes = res[hashtag]
            counter = 0
            for time in allTimes:
                if time >= startTime and time < startTime + timedelta(hours=1):
                    counter += 1
                else:
                    rows.append(counter)
        data[startTime.strftime("%Y-%m-%d %H:%M:%S")] = rows
        startTime += timedelta(hours=1)
    df = pd.DataFrame(data)

    df.to_csv('twitterDump.csv')
