'''
Created on May 4, 2012

@author: jcm
'''

from collections import deque
import time
import logging

class BufferedWriter(object):
    def __init__(self, dumper, flush_number=500, wait_time=500):
        self.dumper = dumper        
        self.buffer = deque()        
        self.stop = False
        self.flush_number = flush_number
        self.wait_time = float(wait_time) / 1000 # sleep needs the parameter in seconds
                
    def push_status(self, status):
        deque.append(status)
    
    def write_process(self):
        out_counter = 0
        logging.info("write process started")
        while not self.stop:
            if len(self.buffer) > 0: 
                element = self.buffer.popleft()
                self.dumper.dump(element)
                out_counter += 1
                if out_counter > self.flush_number:
                    logging.info("written {count} status elements".format(count=out_counter))
                    self.dumper.flush()                            
            else:
                logging.info("no data in the buffer, sleeping for {}ms".format(self.wait_time))
                time.sleep(self.wait_time)
        
        logging.info("writting last {} statuses".format(len(self.buffer)))
        for elem in self.buffer:
            self.dumper.dump(elem)
                                    
        self.dumper.close()
        logging.info("write process ended")
    
    def __call__(self):
        self.write_process()    
