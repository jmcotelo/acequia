__all__=['YamlStatusDumper']
'''
Created on May 4, 2012

@author: jcm
'''

import yaml
from .interfaces import ITwitterStatusDumper 

class YamlStatusDumper(ITwitterStatusDumper):
    def __init__(self, out_fname):        
        self.stream = open(out_fname, mode='wt', encoding="utf-8")
    
    def dump(self, element):
        yaml.dump(element, self.stream, explicit_start=True)
    
    def flush(self):
        self.stream.flush()
    
    def close(self):
        self.stream.close()

class DummyStatusDumper(ITwitterStatusDumper):
	def dump(self, element):
		print(element)
