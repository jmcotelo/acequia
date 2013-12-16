#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on May 4, 2012

@author: jcm
'''
import logging
import time
import argparse
import yaml

from twython import Twython
from twython.exceptions import TwythonError

from acequia.twitter.listeners import TwythonDummyStreamListener
#from acequia.twitter.writer import BufferedAsyncWriter
#from acequia.twitter.dumpers import *
#from acequia.twitter.listeners import *

from threading import Thread

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

def parse_auth_file(auth_fname):
    with open(auth_fname) as stream:
        return yaml.load(stream)

def main(args):
    # Parse auth file
    logging.info("parsing authentication data from {}".format(args.authfile))
    auth_data = parse_auth_file(args.authfile)
    lang_filter = args.lang

    # Get the individual values
    consumer_key = auth_data['consumer']['key']
    consumer_secret = auth_data['consumer']['secret']
    oauth_token = auth_data['oauth']['token']
    oauth_token_secret = auth_data['oauth']['token_secret']

    # instance the Twitter API wrapper
    twitter = Twython(consumer_key, consumer_secret, oauth_token, oauth_token_secret)

    # Get the tracking params
    users = args.follow
    terms = args.terms

    # compose the track param
    track_terms = list(terms)
    track_terms.extend(users)

    # generate the follow-ids
    logging.info("acquiring valid userids from twitter for {} users".format(len(users)))
    screen_names = [user[1:] for user in users]
    try:
        user_objects = twitter.lookup_user(screen_name=','.join(screen_names))
    except TwythonError:
        logging.warn("No valid users found for streaming follow")
        user_objects = []

    follow_ids = [user_obj['id'] for user_obj in user_objects]
    
    # create the stream listener
    logging.info("instancing stream listener (lang_filter={})".format(lang_filter))
    streamer = TwythonDummyStreamListener(consumer_key, consumer_secret, oauth_token, oauth_token_secret, lang_filter)

    # start the stream listener
    logging.info("starting twitter streaming fitering with {} terms, {} users and following {} userids".format(len(terms), len(users), len(follow_ids)))
            
    # # create a background thread for doing the writter dirty work
    # th = Thread(target=writer, name='BackgroundWriter')
    
    # # start the whole thing in separate threads    
    # th.start()
    # logging.info("starting twitter streaming fitering with {} terms, {} users and following {} userids".format(len(terms), len(users), len(follow_ids)))
    # stream.filter(follow=follow_ids, track=track_terms, async=True)
    
    try:
        streamer.statuses.filter(track=track_terms, follow=follow_ids)
        while True:   
            time.sleep(86400) # Wait 'indefinitely' but capture the ctrl-c            
    except KeyboardInterrupt:
        #writer.stop_process()
        logging.info("disconnecting from twitter stream")
        streamer.disconnect()
        #th.join()
    
    logging.info("Have a nice day!")            

def configure_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o','--output', 
                        help='file where all the captured data goes (default=out.yaml)',
                        type=str,
                        default='out.yaml')
    
    parser.add_argument('-l','--lang', 
                        help='lang of the captured tweets (default=es)')
                        
    parser.add_argument('terms', 
                        help='terms to be tracked',
                        nargs='+')
                                                
    parser.add_argument('-f','--follow', 
                        help='users to be followed',
                        nargs='*',
                        default=[])
                        
    auth_group = parser.add_argument_group('auth')
    parser.add_argument('-af','--authfile', 
                        help='file with oAuth tokens',
                        required = True)
    
                        
    return parser

if __name__ == "__main__":
    parser = configure_argparse()
    configure_logging()
    main(parser.parse_args())
