__all__=['BufferedWriter']
'''
Created on May 4, 2012

@author: jcm
'''

from collections import deque
import time
import logging

from .interfaces import IStatusWriter

class BufferedAsyncWriter(IStatusWriter):
    cname = __name__ + '.BufferedAsyncWriter'
    def __init__(self, dumper, flush_number=500, wait_time=1.0):
        self.dumper = dumper        
        self.buffer = deque()        
        self.stop = False
        self.flush_number = flush_number
        self.wait_time = wait_time
        self.logger  = logging.getLogger(self.cname)
                
    def push_status(self, status):
        if not self.stop:
            self.buffer.append(status)
    
    def start_process(self):
        out_counter = 0
        self.logger.info("write process started")
        while not self.stop:
            if len(self.buffer) > 0: 
                element = self.buffer.popleft()
                self.dumper.dump(element)
                out_counter += 1
                if out_counter > self.flush_number:
                    self.logger.info("written {count} status elements".format(count=out_counter))
                    self.dumper.flush() 
                    out_counter = 0                           
            else:
                #self.logger.info("no data in the buffer, sleeping for {}s".format(self.wait_time))
                time.sleep(self.wait_time)
        
        logging.info("writting last {} statuses".format(len(self.buffer)))
        for elem in self.buffer:
            self.dumper.dump(elem)
                                    
        self.dumper.close()
        self.logger.info("write process stopped")        
    
    def stop_process(self):
        self.logger.info("stopping writing process: no more status are being accepted")
        self.stop = True        
    
    def __call__(self):
        self.start_process()
        