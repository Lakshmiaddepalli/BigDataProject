import sys
from TwitterAPI import TwitterAPI


consumer_key = "3i9BvgEo4aUIvgHYaMt8EEO3S"
consumer_secret = "Az5psrtHtCx2m2roG5SQW0F7kYAuF9FrK9oJCNMPZcfT0ctvzn"

access_token_key = "374160268-ivHxXDPUNtX12YdgFu0xVlezJg1HZYYUOPefzFrQ"
access_token_secret = "0njGXY9sCy6Wt3IPjlm9m7I3ZMN7zHfCQYWasxxRMPszi"

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

TRACK_TERM = sys.argv[-1]

while True:
    r = api.request('statuses/filter', {'track': TRACK_TERM})
    for item in r:
        print(item['text'], item['created_at'] if 'text' in item else item)


