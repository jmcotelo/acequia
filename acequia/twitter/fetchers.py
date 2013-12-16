'''
Created on Dec 16, 2012

@author: jcm
'''
import logging
from twython import Twython, TwythonError

from .listeners import TwythonDummyStreamListener

class TwitterStreamingFetcher():
	cname = __name__ + '.TwitterStreamingFetcher'
	
	def __init__(self, auth_data):
		# update the auth data
		self._update_auth_data(auth_data)
		# instance the Twitter API wrapper
		self.twitter_api = Twython(self.consumer_key, self.consumer_secret, 
								   self.oauth_token,  self.oauth_token_secret)
		# instance the logger
		self.logger = logging.getLogger(self.cname)

		# internal state
		self.running = False

	def _update_auth_data(self, auth_data):
		self.consumer_key = auth_data['consumer']['key']
		self.consumer_secret = auth_data['consumer']['secret']
		self.oauth_token = auth_data['oauth']['token']
		self.oauth_token_secret = auth_data['oauth']['token_secret']

	def _get_follow_ids(self, users):     	
	# generate the follow-ids    
		if len(users) > 0: 
			self.logger.info("acquiring valid userids from twitter for {} users".format(len(users)))
			screen_names = [user[1:] for user in users]
			try:
				user_objects = self.twitter_api.lookup_user(screen_name=','.join(screen_names))
			except TwythonError:
				self.logger.warn("No valid users found for streaming follow")
				user_objects = []

			return [user_obj['id'] for user_obj in user_objects]

		return []

	def fetch(self, terms, users, lang_filter=None):
		# compose the track param
		track_terms = list(terms)
		track_terms.extend(users)
		
		# get the user ids to follows    	
		follow_ids = self._get_follow_ids(users)

		# create the stream listener
		self.logger.info("instancing stream listener (lang_filter={})".format(lang_filter))
		self.streamer = TwythonDummyStreamListener(self.consumer_key, self.consumer_secret, 
												 	self.oauth_token, self.oauth_token_secret, 
													lang_filter)

		if len(follow_ids) > 0:
			self.logger.info("starting twitter streaming fitering with {} terms".format(len(terms)))
			self.logger.info("starting twitter streaming fitering with {} terms, {} users and following {} userids".format(len(terms), len(users), len(follow_ids)))
		else:
			follow_ids = None

		# start the retrieving process
		self.streamer.statuses.filter(track=track_terms, follow=follow_ids)

		self.running = True

	def stop(self):
		if self.running:
			self.streamer.disconnect()
