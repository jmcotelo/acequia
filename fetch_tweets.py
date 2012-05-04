#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on May 4, 2012

@author: jcm
'''
import logging
import time

import tweepy
from tweepy.streaming import Stream

from acequia.twitter.writer import BufferedAsyncWriter
from acequia.twitter.dumpers import YamlStatusDumper
from acequia.twitter.listeners import TweepyWriterListener

from threading import Thread

users = ['@movistar_es', '@somosyoigo', '@vodafone_es', '@orange_es', '@simyo_es', '@jazztel_es', '@pepephone','@tuenti_movil']
terms = ['vodafone', 'yoigo', 'simyo', 'pepephone', 'tuenti_movil', 'jazztel', 'orange', 'movistar']

def configure_logging():    
    # log to file including debug
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s;%(threadName)s;%(name)s;%(levelname)s;%(message)s',
                    #datefmt='%m-%d %H:%M',
                    filename='fetch_tweets.log',
                    filemode='w')    
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s [%(threadName)s|%(name)s] %(levelname)s: %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)


def authenticate_tweepy():
    # == OAuth Authentication ==
    #
    # This mode of authentication is the new preferred way
    # of authenticating with Twitter.

    # The consumer keys can be found on your application's Details
    # page located at https://dev.twitter.com/apps (under "OAuth settings")
    consumer_key="UjKxPuKnA0dxOePQxa7w"
    consumer_secret="uWUGbDbcA19RM0xmzLgPiz4DcVWNtNUzZdmkWQAJfk"

    # The access tokens can be found on your applications's Details
    # page located at https://dev.twitter.com/apps (located 
    # under "Your access token")
    access_token="276989797-hVU39g4QgXjAejCXSiOmVs9go9pTferPPAAyBYBq"
    access_token_secret="Zoj8shrrV30RzwxfDc2L7BY5qFzMAsCVb2ZqBmdOc"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)    
    
    return auth

def main():
    # create the credentials via OAuth (see Devs section in Twitter)
    auth = authenticate_tweepy()
    api = tweepy.API(auth)
    
    # create the dumper and the writer    
    dumper = YamlStatusDumper('full.yaml')
    writer = BufferedAsyncWriter(dumper)
    
    # create a background thread for doing the writter dirty work
    th = Thread(target=writer, name='BackgroundWriter')    
    
    # create the listener and the stream object
    listener = TweepyWriterListener(writer=writer, lang_filter='es')
    stream = Stream(auth, listener)
    
    # compose the track param
    track_terms = list(terms)
    track_terms.extend(users)
    
    # generate the follow-ids
    logging.info("acquiring valid userids from twitter for {} users".format(len(users)))
    follow_ids = []
    for usr_name in users:
        try:
            follow_ids.append(api.get_user(usr_name).id)
        except tweepy.TweepError:
            pass
           
    # start the whole thing in separate threads    
    th.start()
    logging.info("starting twitter streaming fitering with {} terms, {} users and following {} userids".format(len(terms), len(users), len(follow_ids)))
    stream.filter(follow=follow_ids, track=track_terms, async=True)             
    
    try:
        while True:   
            time.sleep(10000000000) # Wait 'indefinitely' but capture the ctrl-c            
    except KeyboardInterrupt:
        writer.stop_process()
        logging.info("disconnecting from twitter stream")
        stream.disconnect()
        th.join()
    
    logging.info("Have a nice day!")            

if __name__ == "__main__":
    configure_logging()
    main()