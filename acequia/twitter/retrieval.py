'''
Created on Dec 20, 2012

@author: jcm
'''
import logging
import datetime
import time

from threading import Thread

from .fetchers import TwitterStreamingFetcher
from .subprocess import SubProcessWrapper
from .interfaces import ISubProcess
from .graph import GraphAnalyzer, GraphBuilder, GraphDumper
from .parsing import StatusDataParser

class TwitterAdaptiveRetriever:
	cname = __name__ + '.TwitterAdaptiveRetriever'
	
	def __init__(self, auth_data, statuses_dir, graphs_dir, termlists_dir, lang_filter=None):
		# store the target dir for holding data
		self.statuses_dir = statuses_dir
		self.graphs_dir = graphs_dir
		self.terms_dir = termlists_dir

		# instance the logger
		self.logger = logging.getLogger(self.cname)

		# instance the internal stream fetcher
		self.fetcher = TwitterStreamingFetcher(auth_data, statuses_dir, lang_filter)

		# internal state
		self.running = False
		self.background_thread = None

	def start(self, seed_set, start_users, period=3600, an_window=12, max_terms=15):
		# initially start the fetching process
		self.fetcher.start(seed_set, start_users)

		self.logger.info("Adatptive retrieval process started (p={}, w={}, max_t={})".format(period, an_window, max_terms))
		self.running = True

		def thread_running(retriever, seed_set, an_window, max_terms, period):
			t_prev = time.time()
			while self.running:
				elapsed_time = time.time() - t_prev
				if elapsed_time >= period:
					logging.info("starting new iteration")
					retriever._iteration(seed_set, an_window, max_terms)
					t_prev = time.time()
				time.sleep(1.0)			

			self.logger.info("performing a last analysis stage")
			#perform an additional last analysis stage
			self._anlysis_stage(seed_set, an_window)

		self.background_thread = Thread(target=thread_running, args=(self, seed_set, an_window, max_terms, period))
		self.background_thread.start()

	def stop(self):
		self.logger.info("Stopping adaptive retrieval process")		
		self.running = False		
		self.fetcher.stop()
		self.background_thread.join()

	# private helper methods
	def _iteration(self, seed_set, an_window, max_terms):
		# stop fetching process
		self.fetcher.stop()		
		
		# perform the analysis stage
		ranking = self._anlysis_stage(seed_set, an_window)

		# get the next term set		
		term_set, users = self._get_next_termset(seed_set, ranking, max_terms)

		# start a new fetching process
		self.fetcher.start(term_set, users)		
	
	def _anlysis_stage(self, seed_set, an_window):
		# parse data from the last n status files in the an_window
		parsed_data_gen = StatusDataParser.load_n(self.statuses_dir, n=an_window)
		
		# build a graph with the parsed data
		graph = GraphBuilder.build_graph(parsed_data_gen, seed_set)
		
		# analyze the graph
		ranking = GraphAnalyzer.get_relevance_ranking(graph)
		
		# update the graph nodes with their relevance ranking and dump the graph into a file
		for node_id, value in ranking:
			graph.node[node_id]['ranking'] = value
		t_now = datetime.datetime.now()		
		timestamp = t_now.strftime("%Y-%m-%d_%H-%M-%S")
		fpath = "{}/graph{}.gexf".format(self.graphs_dir, timestamp)
		GraphDumper.dump_graph(graph,fpath)
		
		return ranking

	def _get_next_termset(self, seed_set, ranking, max_terms):
		# get the most 'max_terms' relevant terms from the ranking, preserving the seed terms
		new_terms = [term for term, value in ranking if term not in seed_set][:max_terms - len(seed_set)]
		term_set = set(seed_set)
		term_set.update(new_terms)
		self.logger.debug("ranking={}".format(ranking[:max_terms]))
		self.logger.debug("seed_set={}".format(seed_set))
		self.logger.debug("term_set={}".format(term_set))
		self.logger.debug("new_terms={}".format(new_terms))
		
		# dump the terms 
		t_now = datetime.datetime.now()		
		timestamp = t_now.strftime("%Y-%m-%d_%H-%M-%S")
		fpath = "{}/terms{}.txt".format(self.terms_dir, timestamp)
		with open(fpath, 'wt', encoding='utf-8') as fd:
			fd.writelines('{}\n'.format(term) for term in term_set)
		
		# get users separately
		users = [t for t in term_set if t.startswith('@')]
		return term_set, users
