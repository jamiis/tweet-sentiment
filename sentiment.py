from pyspark import SparkContext

from collections import defaultdict
from operator import add
from pprint import pprint

sentimentdict = defaultdict(None)

with open('effectwordnet/goldStandard.tff') as f:
    for line in f:
        # split on tabs
        line = line.split('\t')
        # get words and the sentiment for those words
        sentiment = line[1]
        words = line[2].split(',')
        for word in words:
            # add each word to the sentiment dictionary
            if sentiment != 'Null':
                sentimentdict[word] = sentiment[0]

if __name__ == "__main__":
    sc = SparkContext(appName='TweetSentiment')

    # TODO change text file and partition count
    partitions = 30
    text = sc.textFile('data/tweets.1000000.sample', partitions)

    # remove anything that isn't a tweet (e.g. java logging)
    tweets = text.filter(lambda line: line and '@' == line[0])

    # all tweets fall within a 10 hour timespan
    hours = 10.0
    partitions_per_hr = tweets.getNumPartitions() / hours

    def set_hour_key(partition, tweets):
        '''assigns tweets an hour key'''
        hour = int(partition/partitions_per_hr)
        yield hour, tweets

    tweets.coalesce(partitions)

    # TODO implement yo
    def get_sentiment(tweet):
        return 1

    def extract_tweet(line):
        '''lop off twitter username'''
        tweet = line.split(" - ", 1)
        return tweet[1] if len(tweet) > 1 else None

    sentiment_per_hr = tweets\
        .map(extract_tweet)\
        .map(get_sentiment)\
        .mapPartitionsWithIndex(set_hour_key)\
        .flatMap(lambda t: [(t[0], sentiment) for sentiment in t[1]])\
        .reduceByKey(add)\
        .collect()

    pprint(sentiment_per_hr)
