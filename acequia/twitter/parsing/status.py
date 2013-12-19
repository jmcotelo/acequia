'''
Created on Dec 19, 2012

@author: jcm
'''

import os
from collections import namedtuple
from itertools import chain

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

class StatusDataParser:
	DefaultDataStructure = namedtuple('StatusData', ['author_name', 'hashtags', 'user_mentions'])	
	
	@classmethod
	def _default_strategy(cls, data):
		# get author screen_name
		author_name = data['user']['screen_name']
		
		# get the entities
		entities = data['entities']
		
		# get hashtags and mentions
		hashtags = [h['text'] for h in entities['hashtags']]
		user_mentions = [m['screen_name'] for m in entities['user_mentions']]
		if len(hashtags) == 0: hashtags = None
		if len(user_mentions) == 0: user_mentions = None
		
		return cls.DefaultDataStructure(author_name, hashtags, user_mentions)

	@classmethod
	def load_n(cls, status_data_dir, extract_func=None, n=10):		
		proc_func = extract_func if extract_func else cls._default_strategy
		# get files
		src_dir = os.path.normpath(status_data_dir)
		data_fpaths = ['{}/{}'.format(src_dir, fname) for fname in os.listdir(src_dir) if fname.endswith('.yaml')]
		selected_fpaths = sorted(data_fpaths)[-n:]
		parsing_generators = [cls._data_extractor_generator(fpath, proc_func) for fpath in selected_fpaths]
		return chain.from_iterable(parsing_generators)

	@classmethod
	def _data_extractor_generator(cls, fpath, proc_func):
		with open(fpath) as fd:
			# create generator and skip header
			statuses = yaml.load_all(fd, Loader=Loader)			
			statuses.__next__()
			for status_data in statuses:
				parsed_data = proc_func(status_data)
				yield parsed_data


		

