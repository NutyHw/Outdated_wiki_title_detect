import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

df = pd.read_csv('./tweets.csv')
df = df.dropna()
df['created_at'] = df['created_at'].apply(lambda date: parse(date))
df['hashtags'] = df['hashtags'].apply(lambda s: s.strip(']').strip('[').split(','))
df['hashtags'] = df['hashtags'].apply(lambda s: [ hashtag.strip() for hashtag in s ])
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
    return set(df['hashtags'])

def createHashtagSignal(allHashtags,df):
    allDates = df['created_at'].sort_values().tolist()
    dates = list()
    start = allDates[0]
    end = allDates[-1]

    while start < end:
        dates.append(start)
        start += relativedelta(hours=3)

    for i in range(len(allHashtags))
        hashtag = allHashtags[i]
        res = dict()
        cursor = 0
        tmpDf = df[ df['hashtags'] == hashtag ]
        res[i] = [ 0 ]
        res['date'] = [ dates[0].strftime('%D-%H:%M') ]

        for index,row in tmpDf.iterrows():
            if cursor == len(dates):
                break

            while row['created_at'] > dates[cursor+1]:
                cursor += 1
                res['date'].append(dates[cursor].strftime('%D-%H:%M'))
                res[i].append(0)

            if row['created-at'] >= dates[cursor]:
                res[i][-1] += 1

        for i in range(cursor+1,len(dates)):
            res[i].append(0)

        fname = f'signal-{i}'
        with open(fname,'w') as f:
           json.dump(res,f)

if __name__ == '__main__':
    allHashtags = list()
    with open('allHastags.txt') as f:
        allHashtags = [ line.strip() for line in f.read().splitlines() ]
    createHashtagSignal(allHashtags,df)
