import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import subprocess
import json
import os
import sys

configjson = {}
arg = sys.argv
# read config file
with open(arg[1],'r') as configfile:
	for line in configfile:
		configjson = json.loads(line)

# setup api
auth = OAuthHandler(configjson['consumer_key'],configjson['consumer_secret'])
auth.set_access_token(configjson['access_token'],configjson['access_secret'])

api = tweepy.API(auth)
keywords = arg[3].split(',')
topic = arg[2]
# check if topic folder exist
if not os.path.exists(topic):
	os.makedirs(topic)
# change active directory to topic directory
os.chdir(topic)

global counter
counter = 0

# make the file different for each hour
import datetime
now_time = datetime.datetime.now()
global day
day = now_time.day
global month
month = now_time.month
global year
year = now_time.year
global hour
hour = now_time.hour

global twitter_dump_file
twitter_dump_file = open('{}-{:04d}{:02d}{:02d}-{:02d}.tweet'.format(topic,year,month,day,hour),'a')

class MyListener(StreamListener):
	def on_data(self,data):
		try:
			# we interested in english language only
			#jsondata = json.loads(data)

			# compare with the old time
			current_time = datetime.datetime.now()
			cur_day = current_time.day
			cur_hour = current_time.hour
			cur_month = current_time.month
			cur_year = current_time.year
			global day
			global hour
			global month
			global year
			global twitter_dump_file
			if cur_hour!=hour:
				day = cur_day
				month = cur_month
				year = cur_year
				hour = cur_hour
				twitter_dump_file = open('{}-{:04d}{:02d}{:02d}-{:02d}.tweet'.format(topic, year, month, day, hour),'a')

			"""
			if 'lang' in jsondata.keys() and jsondata['lang']=='en':
				# we are interested in data that have cascade
				# expanded_url, user_mentions, retweeted_status, quoted_status, in_reply_to_status_id
				if len(jsondata['entities']['urls'])>0  \
				or len(jsondata['entities']['user_mentions']) > 0 \
				or jsondata['in_reply_to_status_id'] is not None  \
				or jsondata['is_quote_status'] \
				or 'retweeted_status' in jsondata.keys():
					twitter_dump_file.writelines(data)
			"""
			twitter_dump_file.write(data)
#			global counter
#			if(counter>10000):
#				counter=0
#				print('flush file')
#			counter+=1
		except BaseException as e:
			print('Error on data {}'.format(str(e)))
		return True
	def on_error(self,status):
		print(status)
		return True

# stream
twitter_stream = Stream(auth,MyListener())
# follow user stream
# twitter_stream.filter(follow = ['292432955'])
# tracking hashtag
twitter_stream.filter(track = keywords)

