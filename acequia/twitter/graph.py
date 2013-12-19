'''
Created on Dec 19, 2012

@author: jcm
'''

class GraphBuilder:
	@classmethod
	def build_graph(cls, data, build_func=None):
		graph = None
		for d in data:
			build_func(g, d)
		return graph

