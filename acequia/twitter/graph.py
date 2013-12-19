'''
Created on Dec 19, 2012

@author: jcm
'''
import networkx as nx

class GraphBuilder:
	# the default building strategy supposes that the item comes in a DefaultDataStucture
	# object used by _default_strategy extraction process defined in parsing.status
	@classmethod
	def _builder_strategy(cls, g, item):
		pass

	# the selection default behaviour is quite straightforward: select items that
	# uses any term coming from the seed set
	@classmethod
	def _selector_strategy(cls, g, item, seed_set):
		if not seed_set.isdisjoint(item.term_set):
			return True
		else:
			return False


	@classmethod
	def build_graph(cls, data, seed_set, build_func=None, selector_functor=None):
		# default behaviour 
		builder = build_func if build_func else cls._builder_strategy
		selector = selector_functor if selector_functor else cls._selector_strategy
		# graph instancing
		graph = nx.DiGraph()
		# data stream processing
		for item in data:
			if selector(g, item, seed_set)
				builder(g, item)
		# return the graph
		return graph
