__all__=['YamlStatusDumper', 'DummyStatusDumper']
'''
Created on May 4, 2012

@author: jcm
'''

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from .interfaces import ITwitterStatusDumper 

class YamlStatusDumper(ITwitterStatusDumper):
    def __init__(self, out_fname, header):        
        self.stream = open(out_fname, mode='wt', encoding="utf-8")
        self.dump(header)
    
    def dump(self, element):
        yaml.dump(element, self.stream, Dumper=Dumper, explicit_start=True)
    
    def flush(self):
        self.stream.flush()
    
    def close(self):
        self.stream.close()

class DummyStatusDumper(ITwitterStatusDumper):
	def dump(self, element):
		tweet = "@{author}: {text}".format(author=element['user']['screen_name'], text=element['text'])
		print(tweet)
