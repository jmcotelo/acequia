'''
Created on Dec 13, 2012

@author: jcm
'''
from twython import TwythonStreamer
import logging

from guess_language import guess_language, UNKNOWN

class TwythonDummyStreamListener(TwythonStreamer):
	cname = __name__ + '.TwythonDummyStreamListener'
	def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret, lang_filter=None, **kwargs):
		super().__init__(app_key, app_secret, oauth_token, oauth_token_secret, **kwargs)
		self.logger = logging.getLogger(self.cname)
		self.lang_filter = lang_filter

	def on_success(self, status_data):		
		# check if the message has any text		
		if 'text' in status_data:
			valid = True
			if self.lang_filter:
				inferred_lang = guess_language(status_data['text'])
				valid = True if (inferred_lang == self.lang_filter or inferred_lang == UNKNOWN) else False				
			
			if valid:
				tweet = "@{author}: {text}".format(author=status_data['user']['screen_name'], text=status_data['text'])
				self.logger.debug(tweet)

	def on_error(self, status_code, data):
		self.logger.error("problems while streaming, error code {}:{}".format(status_code, data))		
		
	def on_limit(self, data):
	    self.logger.warn("limitiation notice from twitter: {}".format(data['track']))

	def on_disconnect(self, data):
		stream_name, code, reason = data['stream_name'], data['code'], data['reason']
		self.logger.warn("Disconnected stream {} from twitter: {} - {}".format(stream_name, code, reason))
		
	def on_timeout(self):
		self.logger.warn("twitter connection timed out")

class TwythonQueuePusherStreamListener(TwythonStreamer):
	cname = __name__ + '.TwythonDummyStreamListener'
	def __init__(self, app_key, app_secret, oauth_token, oauth_token_secret, queue, lang_filter=None, **kwargs):
		super().__init__(app_key, app_secret, oauth_token, oauth_token_secret, **kwargs)
		self.logger = logging.getLogger(self.cname)
		self.lang_filter = lang_filter
		self.queue = queue

	def get_queue(self):
		return self.queue

	def on_success(self, status_data):		
		# check if the message has any text		
		if 'text' in status_data:
			valid = True
			if self.lang_filter:
				inferred_lang = guess_language(status_data['text'])
				valid = True if (inferred_lang == self.lang_filter or inferred_lang == UNKNOWN) else False				
			
			if valid:
				tweet = "@{author}: {text}".format(author=status_data['user']['screen_name'], text=status_data['text'])
				self.queue.put(status_data, False)

	def on_error(self, status_code, data):
		self.logger.error("problems while streaming, error code {}:{}".format(status_code, data))		
		
	def on_limit(self, data):
	    self.logger.warn("limitiation notice from twitter: {}".format(data['track']))

	def on_disconnect(self, data):
		stream_name, code, reason = data['stream_name'], data['code'], data['reason']
		logging.warn("Disconnected stream {} from twitter: {} - {}".format(stream_name, code, reason))
		
	def on_timeout(self):
		self.logger.warn("twitter connection timed out")
