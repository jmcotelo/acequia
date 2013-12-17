'''
Created on Dec 16, 2012

@author: jcm
'''
import logging
from twython import Twython, TwythonError

from .listeners import TwythonQueuePusherStreamListener
from .interfaces import ISubProcess

from multiprocessing import Process, Event, SimpleQueue, Queue
from threading import Thread

import signal

import time

class TwitterStreamingFetcher():
	cname = __name__ + '.TwitterStreamingFetcher'
	
	def __init__(self, auth_data):
		# update the auth data
		self._update_auth_data(auth_data)

		# instance the logger
		self.logger = logging.getLogger(self.cname)

		# internal state
		self.running = False

	def _update_auth_data(self, auth_data):
		self.consumer_key = auth_data['consumer']['key']
		self.consumer_secret = auth_data['consumer']['secret']
		self.oauth_token = auth_data['oauth']['token']
		self.oauth_token_secret = auth_data['oauth']['token_secret']

	def fetch(self, terms, users, lang_filter=None):
		self.shared_queue = Queue()		
		self.stream_sp = StreamSubprocess(terms, users, lang_filter, self.consumer_key,	self.consumer_secret, 
											self.oauth_token, self.oauth_token_secret, self.shared_queue)
		stream_spw = SubProcessWrapper(self.stream_sp)
		self.stream_kill_event = stream_spw.get_kill_event()
		self.stream_proc = Process(target=stream_spw, name = stream_spw.name)
		self.stream_proc.start()	
		self.running = True
		
		self.logger.info("Fetching process started")		

	def stop(self):		
		if self.running:			
			self.stream_kill_event.set()
			# queue cleanup
			try:
				while True:
					data = self.shared_queue.get_nowait()						
					#print(data)
			except:
				pass
			
			self.stream_proc.join()


class StreamSubprocess(ISubProcess):
	cname = __name__ + '.StreamSubprocess'
	name = 'StreamSubprocess'
	
	def __init__(self, terms, users, lang_filter, consumer_key, consumer_secret, oauth_token, oauth_token_secret, queue):
		# instance the logger
		self.logger = logging.getLogger(self.cname)	
		
		# store some data
		self.terms = terms		
		self.users = users

		# instance the Twitter API wrapper
		self.twitter_api = Twython(consumer_key, consumer_secret, 
								   oauth_token,  oauth_token_secret)

		# instance the stream listener
		self.logger.info("instancing stream listener (lang_filter={})".format(lang_filter))
		self.streamer = TwythonQueuePusherStreamListener(consumer_key, consumer_secret, 
												 		oauth_token, oauth_token_secret, 
														queue, lang_filter)

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
	
	def run(self):
		# get the user ids to follows    	
		follow_ids = self._get_follow_ids(self.users)

		# compose the track param
		self.track_terms = list(self.terms)
		self.track_terms.extend(self.users)	

		self.logger.info("starting twitter streaming fitering with {} terms, {} users and following {} userids".format(len(self.terms), len(self.users), len(follow_ids)))
		if len(follow_ids) == 0:			
			follow_ids = None

		# start the retrieving process
		self.streamer.get_queue().cancel_join_thread()		
		self.streamer.statuses.filter(track=self.track_terms, follow=follow_ids)

	def stop(self):
		self.streamer.disconnect()


class SubProcessWrapper:
	cname = __name__ + '.SubProcessWrapper'
	def __init__(self, target, name=None):
		self.target = target
		self.running = False
		self.name = name if name else target.name
		self.kill_event = Event()
		self.logger = logging.getLogger(self.cname)

	def run(self):
		self.logger.info("starting SubProcessTask: {}".format(self.name))
		th = Thread(target=self.target, name=self.target.name)
		th.start()		
		signal.signal(signal.SIGINT, signal.SIG_IGN)
		self.kill_event.wait()
		self.logger.info("stopping SubProcessTask: {}".format(self.name))
		self.target.stop()		

	def __call__(self):
		self.run()

	def get_kill_event(self):
		return self.kill_event