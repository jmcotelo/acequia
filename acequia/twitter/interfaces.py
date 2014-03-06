'''
Created on May 4, 2012

@author: jcm
'''

class IStatusWriter(object):
	def push_status(self, status_data):
		pass
	
class ITwitterStatusDumper(object):
	def dump(self, element):
		pass
	
	def flush(self):
		pass
	
	def close(self):
		pass

class ISubProcess:
	def task_name(self):
		pass
	
	def run(self):
		pass

	def stop(self):
		pass

	def __call__(self):
		self.run()
		