from twython import TwythonStreamer
import json
import csv
import sys


# Filter out unwanted data
def process_tweet(tweet):
    try:
        d = {}
        #d['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
        d['text'] = tweet['text']
        #d['user'] = tweet['user']['screen_name']
        #d['user_loc'] = tweet['user']['location']
        return d
    except Exception as err:
        exception_type = type(err).__name__
        print("process_tweet " + exception_type)
    
    
# Create a class that inherits TwythonStreamer
class MyStreamer(TwythonStreamer):

    # Received data
    def on_success(self, data):
        try:
            # Only collect tweets in English
            if data['lang'] == 'en':
                tweet_data = process_tweet(data)
                self.save_to_csv(tweet_data)
                self.i = self.i + 1
                print(self.i)
                if (self.i > 20000):
                    sys.exit()
        except Exception as err:
            exception_type = type(err).__name__
            print("on_success " + exception_type)

    # Problem with the API
    def on_error(self, status_code, data):
        try:
            print(status_code, data)
            self.disconnect()
        except Exception as err:
            exception_type = type(err).__name__
            print("on_error " + exception_type)
            
        
    # Save each tweet to csv file
    def save_to_csv(self, tweet):
        try:
            with open(r'tweets.csv', 'a', encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(list(tweet.values()))
            with open('tweets.txt', 'a', encoding="utf-8") as out_file:
                out_file.write("--------------------------------------------------------------------------------------\n")
                out_file.write(tweet['text'])
                out_file.write("\n*************************************************************************************\n")
        except Exception as err:
            exception_type = type(err).__name__
            print("save_to_csv " + exception_type)


try:

    with open("twitter_credentials.json", "r") as file:
        creds = json.load(file)


    # Instantiate from our streaming class
    stream = MyStreamer(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'], 
                        creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])

    stream.i = 0

    # Start the stream
    stream.statuses.filter(track='dog')

except Exception as err:
    exception_type = type(err).__name__
    print("main " + exception_type)
