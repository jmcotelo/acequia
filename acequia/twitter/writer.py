__all__=['BufferedWriter']
'''
Created on May 4, 2012

@author: jcm
'''

from collections import deque
import queue
import time
import logging

from .interfaces import IStatusWriter, ISubProcess

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
        self.logger.info("writing process started")
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


class PullBufferedWriter(IStatusWriter, ISubProcess):
    cname = __name__ + '.BufferedAsyncWriter'
    def __init__(self, dumper, ex_buffer, batch_size=100, flush_number=500, wait_time=1.0):
        self.dumper = dumper        
        self.buffer = ex_buffer
        self.stop = False
        self.flush_number = flush_number
        self.wait_time = wait_time
        self.batch_size = batch_size
        self.logger  = logging.getLogger(self.cname)

    def push_status(self, status_data):
        self.dumper.dump(status_data)
                   
    def get_batch_from_buffer(self, max_items):
        batch, item_counter = [], 0
        try:
            while True:
                batch.append(self.buffer.get(block=False))
                item_counter += 1
                if max_items and item_counter == max_items:
                    break                        
        except queue.Empty:
            pass

        return batch         
    
    def run(self):
        out_counter = 0
        self.logger.info("writing process started")
        while not self.stop:
            batch = self.get_data_from_buffer(max_items=self.batch_size)
            if len(batch) > 0:                        
                for data in batch:       
                    self.dumper.dump(data)
                    out_counter += 1
                    if out_counter > self.flush_number:
                        self.logger.info("written {count} status elements".format(count=out_counter))
                        self.dumper.flush() 
                        out_counter = 0                           
            else:
                #self.logger.info("no data in the buffer, sleeping for {}s".format(self.wait_time))
                time.sleep(self.wait_time)
        
        logging.info("writting last {} statuses".format(len(self.buffer)))
        last_batch = self.get_data_from_buffer(max_items=None)
        for data in last_batch:
            self.dumper.dump(data)
                                    
        self.dumper.close()
        self.logger.info("write process stopped")        
    
    def stop(self):
        self.logger.info("stopping writing process: no more status are being accepted")
        self.stop = True        
