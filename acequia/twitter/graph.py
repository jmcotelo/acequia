'''
Created on Dec 19, 2012

@author: jcm
'''
import networkx as _nx
import itertools as _itertools

class GraphBuilder:
	# the default building strategy supposes that the item comes in a DefaultDataStucture
	# object used by _default_strategy extraction process defined in parsing.status
	@classmethod
	def _builder_strategy(cls, g, item):
		# Nodes are create explictily when an edge is added to the graph
		author_id = '@{}'.format(item.author_name)

		if item.hashtags:
			# User -> Hashtag edges
			for tag_id in item.hashtags:
				cls._update_graph_with_edge(g, author_id, tag_id)

			# Hashtag <-> Hashtag edges
			for t1, t2 in _itertools.combinations(item.hashtags, r=2):
				cls._update_graph_with_edge(g, t1, t2)
				cls._update_graph_with_edge(g, t2, t1)

		if item.user_mentions:
			# User -> User edges
			for user_id in item.user_mentions:
				cls._update_graph_with_edge(g, author_id, user_id)

	# helper method to avoid copy/paste repetition
	@classmethod
	def _update_graph_with_edge(cls, g, from_id, to_id):
		arc = (from_id, to_id)
		if arc not in g.edges():
			g.add_edge(*arc, weight=0)
		g[from_id][to_id]['weight'] +=1


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
		g = _nx.DiGraph()
		# data stream processing
		for item in data:
			if selector(g, item, seed_set):
				builder(g, item)
		# return the graph
		return g

class GraphAnalyzer:
	@classmethod
	def get_relevance_ranking(cls, graph, relevance_measure=_nx.pagerank_scipy):		
		if graph:
			# get the relevance values
			relevance = relevance_measure(graph)
			# sort the relevance items for its relevance value. Descending order
			ranking = sorted(relevance.items(), key=lambda x:x[1], reverse=True)
			return ranking
