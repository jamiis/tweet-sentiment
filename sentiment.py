from collections import defaultdict
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
