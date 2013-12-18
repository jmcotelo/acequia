'''
Created on Dec 16, 2012

@author: jcm
'''
import logging
from twython import Twython, TwythonError

from .listeners import TwythonQueuePusherStreamListener
from .interfaces import ISubProcess
from .subprocess import SubProcessWrapper
from .writer import BufferedAsyncWriter, PullBufferedWriter
from .dumpers import DummyStatusDumper

from multiprocessing import Process, SimpleQueue, Queue

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


	def _crate_stream_subprocess(self, terms, users, lang_filter):
		stream_sp = TwythonStreamSubprocess(terms, users, lang_filter, self.consumer_key,	self.consumer_secret, 
									 		self.oauth_token, self.oauth_token_secret, self.shared_queue)
		return self._create_subprocess_wrapper(stream_sp, 'StreamingProcess')
		
	def _create_writer_subprocess(self):
		writer_sp = PullBufferedWriter(DummyStatusDumper(), self.shared_queue)
		return self._create_subprocess_wrapper(writer_sp, 'WritingProcess')
		
	def _create_subprocess_wrapper(self, sp, spw_name=None):
		spw = SubProcessWrapper(sp, spw_name)
		kill_event = spw.get_kill_event()
		p = Process(target=spw, name = spw.name)
		return(p, kill_event)

	def fetch(self, terms, users, lang_filter=None):
		# create the common queue
		self.shared_queue = Queue()
		self.stream_proc, self.stream_kill_event = self._crate_stream_subprocess(terms, users, lang_filter)
		self.writer_proc, self.writer_kill_event = self._create_writer_subprocess()
		
		self.stream_proc.start()
		self.writer_proc.start()

		self.running = True
		
		self.logger.info("Fetching process started")		

	def stop(self):		
		if self.running:
			self.stream_kill_event.set()				
			self.writer_kill_event.set()
			self.writer_proc.join()
			self.stream_proc.join()			
			


class TwythonStreamSubprocess(ISubProcess):
	cname = __name__ + '.TwythonStreamSubprocess'
	name = 'TwythonStreamingProcess'
	
	def __init__(self, terms, users, lang_filter, consumer_key, consumer_secret, oauth_token, oauth_token_secret, shared_queue):
		# instance the logger
		self.logger = logging.getLogger(self.cname)	
		
		# store some data
		self.terms = terms		
		self.users = users
		self.shared_queue = shared_queue

		# instance the Twitter API wrapper
		self.twitter_api = Twython(consumer_key, consumer_secret, 
								   oauth_token,  oauth_token_secret)

		# instance the stream listener
		self.logger.info("instancing stream listener (lang_filter={})".format(lang_filter))
		self.streamer = TwythonQueuePusherStreamListener(consumer_key, consumer_secret, 
												 		oauth_token, oauth_token_secret, 
														shared_queue, lang_filter)

	def task_name(self):
		return self.name

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
		self.streamer.statuses.filter(track=self.track_terms, follow=follow_ids)

	def stop(self):
		self.streamer.disconnect()
		self.shared_queue.close()
