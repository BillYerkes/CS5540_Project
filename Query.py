# Import the Twython class
from twython import Twython
import json

# Load credentials from json file
with open("twitter_credentials.json", "r") as file:
    creds = json.load(file)

# Instantiate an object
twitter = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])

try:
    result = twitter.show_user(screen_name='sueyerkes')
    print(result['name'])
    print(result)
except TwythonError as e:
    print(e)

