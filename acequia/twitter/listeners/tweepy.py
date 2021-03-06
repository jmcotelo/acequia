'''
Created on May 4, 2012

@author: jcm
'''

from tweepy.streaming import StreamListener

import logging
from guess_language import guess_language, UNKNOWN

class TweepyDummyStreamListener(StreamListener):
    cname = __name__ + '.TweepyDummyStreamListener'
    def __init__(self, api=None, lang_filter=None):
        super(TweepyDummyStreamListener, self).__init__(api)
        self.logger = logging.getLogger(self.cname)
        self.lang_filter = lang_filter      
        
    def on_status(self, status):
        accept = True
        if self.lang_filter:
            inferred_lang = guessLanguage(status.text)            
            accept = True if (inferred_lang == self.lang_filter or inferred_lang == UNKNOWN) else False                
            
        if accept:
            tweet = "@{author}: {text}".format(author=status.user.screen_name, text=status.text)
            self.logger.debug(tweet)
            
    def on_error(self, status_code):
        self.logger.error("problems while streaming,error code: {}".format(status_code))
        
    def on_limit(self, track):
        self.logger.warn("limitiation notice from twitter: {}".format(track))
        
    def on_timeout(self):
        self.logger.warn("twitter connection timed out")

class TweepyWriterStreamListener(StreamListener):
    cname = __name__ + '.TweepyWriterStreamListener'
    def __init__(self, api=None, writer=None, lang_filter=None):
        super(TweepyWriterStreamListener, self).__init__(api)
        self.writer = writer
        self.logger = logging.getLogger(self.cname)
        self.lang_filter = lang_filter
                                        
    def on_status(self, status):
        accept = True
        if self.lang_filter:
            inferred_lang = guessLanguage(status.text)            
            accept = True if (inferred_lang == self.lang_filter or inferred_lang == UNKNOWN) else False
        if accept:
            self.writer.push_status(status)
        
    def on_error(self, status_code):
        self.logger.error("problems while streaming,error code: {}".format(status_code))
        
    def on_limit(self, track):
        self.logger.warn("limitiation notice from twitter: {}".format(track))
        
    def on_timeout(self):
        self.logger.warn("twitter connection timed out")