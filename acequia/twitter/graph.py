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
		# Nodes are create explictily when an edge is added to the graph
		author_id = '@{}'.format(item.author_name)

		if item.hashtags:
			for tag_id in item.hashtags:
				arc = (author_id, tag_id)
				if arc not in g.edges():
					g.add_edge(*arc, weight=0)
				g[author_id][tag_id]['weight'] +=1

		if item.user_mentions:
			for user_id in item.user_mentions:
				arc = (author_id, user_id)
				if arc not in g.edges():
					g.add_edge(*arc, weight=0)
				g[author_id][user_id]['weight'] +=1

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
		g = nx.DiGraph()
		# data stream processing
		for item in data:
			if selector(g, item, seed_set):
				builder(g, item)
		# return the graph
		return g
