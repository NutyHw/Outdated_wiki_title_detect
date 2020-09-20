import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

df = pd.read_csv('../data/18-09-2021/tweets.csv')
df = df.dropna()
df['created_at'] = df['created_at'].apply(lambda date: parse(date))

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
    res = dict()
    newDf = df.sort_values(by=['created_at'])

    start = newDf['created_at'][0]

    for hashtag in allHashtags:
        res[hashtag] = [ 0 ]

    start = newDf['created_at'][0]
    res['date'] = [ start.strftime('%D-%H:%M') ]

    for index, row in newDf.iterrows():
        row['hashtags'] = row['hashtags'].strip('[').strip(']').split(',')
        if start + relativedelta(hours=3) > row['created_at'] and row['created_at'] >= start:
            for hashtag in row['hashtags']:
                res[hashtag][-1] += 1
        else:
            start = start + relativedelta(hours=3)
            for hashtag in allHashtags:
                res[hashtag].append(0)
            for hashtag in row['hashtags']:
                res[hashtag][-1] += 1
            res['date'].append(start.strftime('%D-%H:%M'))
    return res

if __name__ == '__main__':
    freq, dates = createTimeWindow()

    tweetsCountPerWeek = {
        'freq' : freq,
        'date' : dates
    }

    tmpDf = pd.DataFrame(tweetsCountPerWeek)
    tmpDf.to_csv('2020-tweetsCount.csv')

    newDf = df[ df['created_at'] > datetime(2020,9,1) ]

    allHashtags = getDistinctHashtags(newDf)
    signal = createHashtagSignal(allHashtags,newDf)

    signal = pd.DataFrame(signal)
    signal.to_csv('2020-09_HashtagSignal.csv')

    with open('allHashtags.txt','w') as f:
        json.dump(allHashtags,f)

