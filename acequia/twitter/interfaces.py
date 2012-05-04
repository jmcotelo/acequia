'''
Created on May 4, 2012

@author: jcm
'''

class IStatusWriter(object):
    def push_status(self, status):
        pass
    
class ITwitterStatusDumper(object):
    def dump(self, element):
        pass
    
    def flush(self):
        pass
    
    def close(self):
        pass
