import json

# Enter your keys/secrets as strings in the following fields
credentials = {}
credentials['CONSUMER_KEY'] = 'JqYGyDuWlqqYA0j3nDwd7scbK'
credentials['CONSUMER_SECRET'] = '1QmKJKGbCmlM0txd17v5bZnRSzzlgG0jssgnYdlu277ZEUFCZP'
credentials['ACCESS_TOKEN'] = '1280519413624500224-kzn1aFNVpB1tKnMVARHQHZj24g7EaJ'
credentials['ACCESS_SECRET'] = 'ct0j0JsP6AjZ97VoghpdlvNfOi5J57aH7Gc04L3VMw4jt'

# Save the credentials object to file
with open("twitter_credentials.json", "w") as file:
    json.dump(credentials, file)
