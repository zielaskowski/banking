#!/usr/bin/env python
#from: https://pycallgraph.readthedocs.io/en/master/guide/filtering.html

from pycallgraph import PyCallGraph
from pycallgraph.output import GraphvizOutput
from db import DB

graphviz = GraphvizOutput(output_file='filter_none.png')

with PyCallGraph(output=graphviz):
    dane = DB()