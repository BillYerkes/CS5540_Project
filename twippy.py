import os
import tweepy as tw
import csv

consumer_key  = 'JqYGyDuWlqqYA0j3nDwd7scbK'
consumer_secret = '1QmKJKGbCmlM0txd17v5bZnRSzzlgG0jssgnYdlu277ZEUFCZP'
access_token = '1280519413624500224-kzn1aFNVpB1tKnMVARHQHZj24g7EaJ'
access_token_secret = 'ct0j0JsP6AjZ97VoghpdlvNfOi5J57aH7Gc04L3VMw4jt'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

search_words = ""

date_since = "2020-01-01" 
date_until = "2020-02-01"


# Collect tweets
tweets = tw.Cursor(api.search,
              q=search_words,
              lang="en",
              since=date_since,
              until=date_until).items(5000)

file = open('tweets.txt', 'a', encoding="utf-8")
#writer = csv.writer(file)

i = 0
# Iterate and print tweets
for tweet in tweets:
    #writer.writerow(list(tweet))
    file.write("*****" + tweet.text + "*****")
    i = i + 1
    print(i)

file.close()

