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
import os

from acequia.twitter import TwitterAdaptiveRetriever

def configure_logging():    
    # log to file including debug
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s;%(processName)s;%(name)s;%(levelname)s;%(message)s',
                    #datefmt='%m-%d %H:%M',
                    filename='fetch_tweets.log',
                    filemode='w')    
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s [%(processName)s|%(name)s] %(levelname)s: %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

def parse_auth_file(auth_fname):
    with open(auth_fname) as stream:
        return yaml.load(stream)

def check_and_create_output_directory(base_dir, subdirs=['statuses', 'graphs', 'terms']):
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)    
    subdir_paths = ['{}/{}'.format(base_dir, subdir) for subdir in subdirs]
    for path in subdir_paths:
        if not os.path.exists(path):
            os.makedirs(path)        
    return subdir_paths

def main(args):    
    # Parse auth file
    logging.info("parsing authentication data from {}".format(args.authfile))
    auth_data = parse_auth_file(args.authfile)
    
    # Get the lang filter
    lang_filter = args.lang

    # Get the output directory
    output_dir = args.output_dir
    subdir_paths = None
    if not os.path.exists(output_dir):        
        logging.info("output directory '{}' does not exists. Creating it.".format(output_dir))
        subdir_paths = check_and_create_output_directory(output_dir)
    elif os.path.isdir(output_dir):
        # check/create subdirs
        subdir_paths = check_and_create_output_directory(output_dir)
    else:
        logging.error("output path '{}' already exists and is not a directory. Aborting".format(output_dir))
        exit()

    logging.info("setting '{}' as output directory.".format(output_dir))

    # Get the tracking params
    users = args.follow
    terms = args.terms            
    
    try:
        retriever = TwitterAdaptiveRetriever(auth_data, *subdir_paths, lang_filter=lang_filter)
        retriever.start(set(terms), users, args.period, args.window, args.max_terms)

        while True:   
            time.sleep(86400) # Wait 'indefinitely' but capture the ctrl-c            
    
    except KeyboardInterrupt:                
        logging.info("stopping retrieval process")
        retriever.stop()        
    
    logging.info("Have a nice day!")            

def configure_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o','--output-dir', 
                        help='file where all the captured data goes (default=output_data)',
                        type=str,
                        default='output_data')
    
    parser.add_argument('-l','--lang', 
                        help='lang of the captured tweets (default=es)')
                        
    parser.add_argument('terms', 
                        help='seed terms to be tracked',
                        nargs='+')
                                                
    parser.add_argument('-f','--follow', 
                        help='users to be followed',
                        nargs='*',
                        default=[])

    parser.add_argument('-p','--period',
                        help='iteration period for adaptive retrieval in secs (default=3600)',
                        type=int,
                        default=3600)

    parser.add_argument('-w','--window',
                        help='quantity of past iterations used for adaptive analysis (default=12)',
                        type=int,
                        default=12)

    parser.add_argument('-t','--max_terms',
                        help='maximum number of terms, including seed terms, used for term list generation (default=15)',
                        type=int,
                        default=15)
                        
    auth_group = parser.add_argument_group('auth')
    parser.add_argument('-af','--authfile', 
                        help='file with oAuth tokens',
                        required = True)
    
                        
    return parser

if __name__ == "__main__":
    parser = configure_argparse()
    configure_logging()
    main(parser.parse_args())
