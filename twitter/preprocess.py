from pymongo import MongoClient
from pymongo import UpdateOne
from dotenv import load_dotenv
import os

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

    db = client[os.getenv('authSource')]

    return db

def processRawTweets(db):
    pipeline1 = [
        { '$match' : { 'full_text' : { '$exists' : True }}},
        { '$unwind' : '$entities' },
        { '$sort' : { 'entities.updated_time' : 1 }},
        { '$group' : { 
            '_id' : { 'id' : '$id' },
            'id' : { '$first' : '$id' },
            'created_at' : { '$first' : '$created_at' },
            'user_id' : { '$first' : '$user_id' },
            'full_text' : { '$first' : '$full_text' },
            'hashtags' : { '$first' : '$hashtags' },
            'user_mentions' : { '$first' : '$user_mentions' },
            'entities' : { '$push' : '$entities' }
        }},
        { '$project' : {
            '_id' : 0
        }}
    ]

    pipeline2 = [
        { '$match' : { 'full_text' : { '$exists' : False }}},
        { '$unwind' : '$entities' },
        { '$sort' : { 'entities.updated_time' : 1 }},
        { '$group' : { 
            '_id' : { 'id' : '$id' },
            'id' : { '$first' : '$id' },
            'created_at' : { '$first' : '$created_at' },
            'user_id' : { '$first' : '$user_id' },
            'user_mentions' : { '$first' : '$user_mentions' },
            'entities' : { '$push' : '$entities' },
            'retweet_from' : { '$first' : '$retweet_from' }
        }},
        { '$project' : {
            '_id' : 0
        }}
    ]
    cursor = db.rawTweets.aggregate(pipeline1, allowDiskUse = True)
    cursor2 = db.rawTweets.aggregate(pipeline2, allowDiskUse = True)

    db.processTweets.bulk_write([
        UpdateOne(
            { 
                'id' : record['id'],
                'created_at' : record['created_at'],
                'user_id' : record['user_id'],
                'full_text' : record['full_text'],
                'hashtags' : record['hashtags'],
                'user_mentions' : record['user_mentions']
            },
            {
                '$push' : { 'entities' : record['entities'] }
            },
            upsert=True
       )
        for record in cursor
    ])

    db.processTweets.bulk_write([
        UpdateOne(
            { 
                'id' : record['id'],
                'created_at' : record['created_at'],
                'user_id' : record['user_id'],
                'user_mentions' : record['user_mentions'],
                'entities' : record['entities'],
                'retweet_from' : record['retweet_from']
            },
            {
                '$push' : { 'entities' : record['entities'] }
            },
            upsert=True
       )
        for record in cursor2
    ])
def processRawUsers(db):
    pipeline = [
        { '$unwind' : '$entities' },
        { '$group' : {
            '_id' : { 'id' : '$id' },
            'id' : { '$first' : '$id' },
            'name' : { '$first' : '$name' },
            'screen_name' : { '$first' : '$screen_name' },
            'location' : { '$first' : '$location' },
            'entities' : { '$push' : '$location' }
        }},
        { '$project' : {
            '_id' : 0
        }}
    ]

    cursor = db.rawUsers.aggregate(pipeline,allowDiskUse=True)
    db.processUsers.bulk_write([
        UpdateOne(
            { 
                'id' : record['id'],
                'name' : record['name'],
                'screen_name' : record['screen_name'],
                'location' : record['location']
            },
            {
                '$push' : { 'entities' : record['entities'] }
            },
            upsert=True
       )
        for record in cursor
    ])

if __name__ == '__main__':
    db = connect()
    processRawTweets(db)
    processRawUsers(db)
