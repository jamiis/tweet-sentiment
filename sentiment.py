from pyspark import SparkContext

from collections import defaultdict
from operator import add
from pprint import pprint
import sys, os

sentiment = defaultdict(lambda: 0)

# parse EffectWordNet
with open('effectwordnet/goldStandard.tff') as f:
    for line in f:
        # split on tabs
        line = line.split('\t')
        # get words and the sentiment for those words
        posneg = line[1][0]
        words  = line[2].split(',')
        for word in words:
            # add each word to the sentiment dictionary
            if   posneg == '+': sentiment[word] =  1
            elif posneg == '-': sentiment[word] = -1 

def get_sentiment(tweet):
    return sum(sentiment[word] for word in tweet.split())

def extract_tweet(line):
    '''lop off twitter username'''
    tweet = line.split(" - ", 1)
    return tweet[1] if len(tweet) > 1 else None

def list_of_hr_sentiment_tups(tup):
    hr = tup[0]
    sentiments = tup[1]
    return [(hr, s) for s in sentiments]\


if __name__ == "__main__":
    sc = SparkContext(appName='TweetSentiment')

    env = os.getenv('ENV', 'dev')
    print 'ENV: ', env

    # TODO change text file and partition count
    if  env == 'dev':
        num_tweets = sys.argv[1] if len(sys.argv) >= 2 else '1000'
        data = 'data/tweets.%s.sample' % num_tweets
    elif env == 'prod':
        data = 's3n://jamis.bucket/Assignment3Tweets-2'

    text = sc.textFile(data)

    # remove anything that isn't a tweet (e.g. java logging)
    # split twitter username from actual tweet
    tweets = text\
        .filter(lambda line: line and '@' == line[0])\
        .map(extract_tweet)\
        .filter(None)\

    tweets.cache()

    # all tweets fall within a 10 hour timespan
    hours = 10.0
    partitions_per_hr = tweets.getNumPartitions() / hours
    def set_hour_key(partition, tweets):
        '''assigns tweets an hour key'''
        hour = int(partition/partitions_per_hr)
        yield hour, tweets

    # calculate sentiment per hour
    sentiment_per_hr = tweets\
        .map(get_sentiment)\
        .mapPartitionsWithIndex(set_hour_key)\
        .flatMap(list_of_hr_sentiment_tups)\
        .reduceByKey(add)\
        .collect()

    pprint(sentiment_per_hr)
