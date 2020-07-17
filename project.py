##############################################################################################################################
##
## CS 5540
## Project Phase 1
## Bill Yerkes
## wy2n6@mail.umkc.edu
##
## Assignment Description:
## • Goal
##      • Collect Tweets using Twitter’s Streaming APIs (e.g., 100K Tweets)
##          • https://developer.twitter.com/en/docs/tutorials/consuming-streaming-data
##          • Search online for documentation; feel free to use any online code
##      • Extract all the hashtags and URLs in the tweets
##          • Write your own code in any language [Python is preferred]
##      • Run the WordCountexample in:
##          • Apache Hadoopand Apache Spark on the extracted hashtags/URLs and collect the output and log files from Hadoop.
##  • What to submit?
##      • Report that include a description for what you have done & screenshots.
##      • Your code and the output and log files
##
## ERROR HANDLING:
##
## OTHER COMMENTS:
##
##############################################################################################################################


from twython import TwythonStreamer
import json
import csv
import sys

##############################################################################################################################
## process_tweet(tweet)
##
## Purpose: Grab only the information in the tweet data we are going to use and discard the rest.
##
##############################################################################################################################

# Filter out unwanted data
def process_tweet(tweet):
    try:
        #create new dictionary
        filterData = {}

        #grab the tweet data we are going to use
        filterData['hashtags'] = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
        filterData['text'] = tweet['text']
        return filterData
    except Exception as err:
        exception_type = type(err).__name__
        print("process_tweet " + exception_type)
    
##############################################################################################################################
## class MyStreamer(TwythonStreamer)
##
## Purpose: Modify the behavior fo the Twython Streamer class to capture the tweet data we need
##
##############################################################################################################################
    
# Create a class that inherits TwythonStreamer
class MyStreamer(TwythonStreamer):

##############################################################################################################################
## on_success(self, data)
##
## Purpose: Once we get a notification of a tween we are ging to first check to see if it is english.
##          If the tweet is in english we are going to save it to a file
##          We are going to stop the stream if we reach our limit we set for ourselves.
##
##############################################################################################################################
    # Received data
    def on_success(self, data):
        try:
            # Only collect tweets in English
            if ((data['lang'] == 'en')):
                #Get the tweet data we want to work with
                tweet_data = process_tweet(data)
                #Save the data to a file(s)
                self.save_to_csv(tweet_data)
                #count how many we have done
                self.tweetCount = self.tweetCount + 1
                #print to the screen so we can see what is going on
                print(self.tweetCount)
                #if we have reach our goal stop.
                if (self.tweetCount >= self.numberOfTweetsLimit):
                    sys.exit()
        except Exception as err:
            exception_type = type(err).__name__
            print("on_success " + exception_type)

##############################################################################################################################
## on_error(self, status_code, data)
##
## Purpose: If we get an error we need to disconent so we do not get banned from Twitter.
##
##############################################################################################################################
    # Problem with the API
    def on_error(self, status_code, data):
            print(status_code, data)
            self.disconnect()

##############################################################################################################################
## save_to_csv(self, tweet
##
## Purpose: Write the tweet out to a file.  We will be writing out to two files a csv and a text file.
##
##############################################################################################################################        
    # Save each tweet to csv file
    def save_to_csv(self, tweet):
        try:
            #append to csv file
            with open(stream.outputFile + '.csv', 'a', encoding="utf-8") as csvFile:
                writer = csv.writer(csvFile)
                writer.writerow(list(tweet.values()))
            #append to text file
            with open(stream.outputFile + '.txt', 'a', encoding="utf-8") as txtFile:
                txtFile.write(tweet['text'])
                txtFile.write("\n\n")
        except Exception as err:
            exception_type = type(err).__name__
            print("save_to_csv " + exception_type)

##############################################################################################################################
## __main__
##
## Purpose: Log into Twitter, set properties, start collecting tweets
##          Twitter credential are stored in json file.  Seperate code used to save credential to the file.
##
##############################################################################################################################

try:

    with open("twitter_credentials.json", "r") as file:
        creds = json.load(file)


    # Instantiate from our streaming class
    stream = MyStreamer(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'], 
                        creds['ACCESS_TOKEN'], creds['ACCESS_SECRET'])

    stream.tweetCount  = 0
    stream.numberOfTweetsLimit = 10000
    stream.filter = 'http'
    stream.outputFile = 'tweets'

    # Start the stream
    stream.statuses.filter(track=stream.filter)

except Exception as err:
    exception_type = type(err).__name__
    print("main " + exception_type)
