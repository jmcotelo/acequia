import logging
import signal
from multiprocessing import Event
from threading import Thread

class SubProcessWrapper:
	cname = __name__ + '.SubProcessWrapper'
	def __init__(self, target, name=None):
		self.target = target
		self.running = False
		self.name = name if name else target.task_name()
		self.kill_event = Event()
		self.logger = logging.getLogger(self.cname)

	def run(self):
		self.logger.info("starting SubProcessTask: {}".format(self.name))
		th = Thread(target=self.target, name=self.target.task_name())
		th.start()		
		signal.signal(signal.SIGINT, signal.SIG_IGN)
		self.kill_event.wait()
		self.logger.info("stopping SubProcessTask: {}".format(self.name))
		self.target.stop()
		th.join()
		self.logger.info("Stopped SubProcessTask: {}".format(self.name))

	def __call__(self):
		self.run()

	def get_kill_event(self):
		return self.kill_event
