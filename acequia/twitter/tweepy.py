__all__=['TweepyWriterListener']
'''
Created on May 4, 2012

@author: jcm
'''

from tweepy.streaming import StreamListener
import logging

class TweepyWriterListener(StreamListener):
    def __init__(self, api=None, writer=None):
        super(TweepyWriterListener).__init__(api=api)
        self.writer = writer    
                                    
    def on_status(self, status):        
        self.writer.push_status(status)
    
    def on_error(self, status_code):
        logging.error("problems while streaming,error code: {}".format(status_code))
    
    def on_limit(self, track):
        logging.warn("limitiation notice from twitter: {}".format(track))
    
    def on_timeout(self):
        logging.warn("twitter connection timed out")