'''
Created on Dec 16, 2012

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
	@classmethod
	def load_n(cls, status_data_dir, n=10):
		# get files
		src_dir = os.path.normpath(status_data_dir)
		data_fpaths = ['{}/{}'.format(src_dir, fname) for fname in os.listdir(src_dir) if fname.endswith('.yaml')]
		selected_fpaths = sorted(data_fpaths)[-n:]
		parsing_generators = [cls._parsing_generator(fpath, lambda x:x['text']) for fpath in selected_fpaths]
		return chain.from_iterable(parsing_generators)

	def _parsing_generator(fpath, parsing_func):
		with open(fpath) as fd:
			# create generator and skip header
			statuses = yaml.load_all(fd, Loader=Loader)			
			statuses.__next__()
			for status_data in statuses:
				parsed_data = parsing_func(status_data)
				yield parsed_data


		

