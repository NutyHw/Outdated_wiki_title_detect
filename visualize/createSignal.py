import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

df = pd.read_csv('../data/18-09-2021/tweets.csv')
df = df.dropna()
df['created_at'] = df['created_at'].apply(lambda date: parse(date))
df['hashtags'] = df['hashtags'].apply(lambda s: s.strip(')').strip('(').split(','))
df = df.explode('hashtags')

def createTimeWindow():
    freq = list()
    dates = list()

    allDates = df['created_at'].sort_values().tolist()
    start = datetime(2020,1,1)
    count = 0

    for date in allDates:
        if date < start:
            continue
        if start + relativedelta(months=1) > date and date >= start:
            count += 1
        else:
            freq.append(count)
            dates.append(start.strftime('%D'))
            start = start + relativedelta(months=7)
            count = 1

    freq.append(count)
    dates.append(start.strftime('%D'))
    return freq, dates

def getDistinctHashtags(newDf):
    allHashtags = set()
    for index,row in newDf.iterrows():
        row['hashtags'] = row['hashtags'].strip('[').strip(']').split(',')
        allHashtags = allHashtags.union(row['hashtags'])
    return allHashtags

def createHashtagSignal(allHashtags,df):
    allDates = df['created_at'].sort_values().tolist()
    dates = list()
    start = allDates[0]
    end = allDates[-1]

    while start < end:
        dates.append(start)
        start += relativedelta(hours=3)

    for hashtag in allHashtags:
        res = dict()
        cursor = 0
        tmpDf = df[ df['hashtags'] == hashtag ]
        res[hashtag] = [ 0 ]
        res['date'] = [ dates[0].strftime('%D-%H:%M') ]

        for row in tmpDf.iterrows():
            if cursor == len(dates):
                break
            if row['created_at'] > dates[cursor+1]:
                cursor += 1
                res['date'].append(dates[cursor].strftime('%D-%H:%M'))
                res[hashtag].append(0)
                continue

            if row['created-at'] >= dates[cursor]:
                res[-1] += 1

        for i in range(cursor+1,len(dates)):
            res[hashtag].append(0)

       fname = f'signal-{hashtag}'
       with open(fname,'w') as f:
           json.dump(res,f)

if __name__ == '__main__':
    newDf = df[ df['created_at'] > datetime(2020,9,1) ]

    allHashtags = getDistinctHashtags(newDf)
    createHashtagSignal(allHashtags,newDf)
