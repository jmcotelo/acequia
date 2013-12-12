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

#import tweepy
#from tweepy.streaming import Stream

from acequia.twitter.writer import BufferedAsyncWriter
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

 
def authenticate_tweepy(consumer_key, consumer_secret, access_token, access_token_secret):
    # == OAuth Authentication ==
    #
    # This mode of authentication is the new preferred way
    # of authenticating with Twitter.

    # The consumer keys can be found on your application's Details
    # page located at https://dev.twitter.com/apps (under "OAuth settings")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth

def basic_authentication(user, pwd):
    tweepy.BasicAuthHandler(user,pwd)

def parse_auth_file(auth_fname):
    with open(auth_fname) as stream:
        return yaml.load(stream)

def main(args):
    # create the credentials via OAuth (see Devs section in Twitter)
    auth = authenticate_tweepy(**parse_auth_file(args.authfile))
    
    #auth = basic_authentication(args.user, args.password)
    api = tweepy.API(auth)
    
    # create the dumper and the writer    
    dumper = YamlStatusDumper(args.output)
    writer = BufferedAsyncWriter(dumper)
    
    # create a background thread for doing the writter dirty work
    th = Thread(target=writer, name='BackgroundWriter')
    
    # hook a closing response printing routine (before instancing)
    def on_closed_hook(self, resp):
        logging.warn("Closed stream from twitter, performing reconnection")

    Stream.on_closed = on_closed_hook        
    
    # create the listener and the stream object
    listener = TweepyWriterListener(writer=writer, lang_filter=args.lang)
    stream = Stream(auth, listener)
    
    users = args.follow
    terms = args.terms
    
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
            logging.warn('User {} not found in Twitter'.format(usr_name))
           
    # start the whole thing in separate threads    
    th.start()
    logging.info("starting twitter streaming fitering with {} terms, {} users and following {} userids".format(len(terms), len(users), len(follow_ids)))
    stream.filter(follow=follow_ids, track=track_terms, async=True)
    
    try:
        while True:   
            time.sleep(86400) # Wait 'indefinitely' but capture the ctrl-c            
    except KeyboardInterrupt:
        writer.stop_process()
        logging.info("disconnecting from twitter stream")
        stream.disconnect()
        th.join()
    
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
