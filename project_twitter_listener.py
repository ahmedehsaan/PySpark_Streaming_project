import requests
import os
import json
import pandas as pd
import csv
import datetime
import dateutil.parser
import unicodedata
from datetime import datetime
import time
import socket 

os.environ['TOKEN'] =  "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAAi08uZ2TTsmCCm3%2BSu9zICPXESiA%3DGc60t2rmLpn4Nbu48EE5SAL718SIZeDc0NpVytEIPMyxXFSqNM"

def auth():
    return os.getenv('TOKEN')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def create_url(keyword, start_date, end_date, max_results = 10):
    search_url = "https://api.twitter.com/2/tweets/search/recent"
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

def connect_to_endpoint(url, headers, params, next_token = None):
    params['next_token'] = next_token
    response = requests.request("GET", url, headers = headers, params = params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


bearer_token = auth()
headers = create_headers(bearer_token)
keyword="premier league lang:en"
end_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=1)
end_time = end_time.isoformat(timespec='milliseconds') + 'Z'

start_time = datetime.datetime.utcnow() - datetime.timedelta(minutes=10)
start_time = start_time.isoformat(timespec='milliseconds') + 'Z'
max_results = 1000

url = create_url(keyword, start_time,end_time, max_results)
json_response = connect_to_endpoint(url[0], headers, url[1])

s = socket.socket()
host = "127.0.0.1"
port =7777
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(5)
clientsocket, address = s.accept()
for data in json_response['data']:
    
    # Get tweet data
    tweet_data = {
        'id': data['id'],
        'text': data['text'],
        'retweet_count': data['public_metrics']['retweet_count'],
        'reply_count': data['public_metrics']['reply_count'],
        'like_count': data['public_metrics']['like_count'],
        'quote_count': data['public_metrics']['quote_count'],
        'impression_count': data['public_metrics']['impression_count'],
        'created_at': data['created_at'],
        'user_id': data['author_id']
    }

    # Get user data
    user_id = data['author_id']
    user_data = next((user for user in json_response['includes']['users'] if user['id'] == user_id), None)
    if user_data:
        tweet_data['user_followers_count'] = user_data['public_metrics']['followers_count']
        tweet_data['user_following_count'] = user_data['public_metrics']['following_count']
        tweet_data['user_tweet_count'] = user_data['public_metrics']['tweet_count']
        tweet_data['username'] = user_data['username']
        tweet_data['name'] = user_data['name']
        tweet_data['user_description'] = user_data['description']
        tweet_data['user_verification'] = user_data['verified']
        tweet_data['user_creation_date'] = user_data['created_at']
    

    # Send tweet data through socket
    json_data = json.dumps(tweet_data)
    print("Sending:", json_data.encode('utf-8'))
    clientsocket.send((json_data + '\n').encode('utf-8'))


clientsocket.close()


