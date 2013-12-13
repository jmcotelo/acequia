'''
Created on Dec 13, 2012

@author: jcm
'''
from twython import TwythonStreamer
import logging

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
				inferred_lang = guessLanguage(status_data['text'])
				valid = True if (inferred_lang == self.lang_filter or inferred_lang == UNKNOWN) else False	
			
			if valid:
				tweet = "@{author}: {text}".format(author=status_data['user']['screen_name'], text=status_data['text'])
				self.logger.debug(tweet)
				print(tweet)
			
	def on_error(self, status_code, data):
		self.logger.error("problems while streaming, error code {}:{}".format(status_code, data))		
		
	#def on_limit(self, track):
	#    self.logger.warn("limitiation notice from twitter: {}".format(track))
		
	def on_timeout(self):
		self.logger.warn("twitter connection timed out")
