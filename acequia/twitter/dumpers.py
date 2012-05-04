__all__=['YamlStatusDumper']
'''
Created on May 4, 2012

@author: jcm
'''

import yaml
import codecs
from interfaces import ITwitterStatusDumper 

class YamlStatusDumper(ITwitterStatusDumper):
    def __init__(self, out_fname):        
        self.stream = codecs.open(out_fname, mode='wb', "utf-8")        
    
    def dump(self, element):
        yaml.dump(element, self.stream)
    
    def flush(self):
        self.stream.flush()
    
    def close(self):
        self.stream.close()
