#!/usr/bin/env python
#
## Benchmark neo4j python interface
#
#
### Tests
#
# *   Transaction costs
# *   Node creation
# *   Relation creation
# *   Node indexing
# *   Node lookup
#
#
# (CC BY-SA 3.0) 2012 Heinrich Hartmann
# 
import timeit, shutil
DEBUG = 0
from neo4j import GraphDatabase, INCOMING, Evaluation

def main():
    
    global db

    try:
        shutil.rmtree('benchmark_db')
    except:
        pass
    db = GraphDatabase('benchmark_db')
    repetitions = 10
    count = 1000

    transaction_benchmark(repetitions)
    node_benchmark(repetitions,count)
    relation_benchmark(repetitions,count)
    traversal_benchmark(repetitions,count)
    indexing_benchmark(repetitions,count)
    lookup_benchmark(repetitions,count)

    db.shutdown()


properties = {'a':['x'*0]*1, 'b': -1 }


def transaction_benchmark(repetitions = 50):
    stmt = """with db.transaction: pass""" 
    setup ="""from __main__ import db"""
    t = timeit.Timer(stmt, setup)
    unit_costs = t.timeit(number=repetitions)/float(repetitions)
    print "Transaction costs:         %.3f msec/trans      %.1f trans/sec    (%d repetitions)." % (1000 * unit_costs, 1./unit_costs, repetitions)
    

def node_benchmark(repetitions = 1, node_count = 10000):
    stmt = """create_node(%d)""" % node_count
    setup = """from __main__ import create_node """

    t = timeit.Timer(stmt, setup)
    unit_time = t.timeit(number=repetitions)/repetitions/node_count
    print "Node creation costs:       %.3f msec/node,      %.1f nodes/sec (%d repetitions, %d nodes)" % (
        1000 * unit_time, 
        1. / unit_time, 
        repetitions, 
        node_count)

def create_node(n = 1):
    with db.transaction:
        for i in range(n):
            db.node(**properties)

def relation_benchmark(repetitions = 1, relation_count = 10000):
    stmt = """create_relations(%d)""" % relation_count
    setup = """from __main__ import create_relations, db"""

    t = timeit.Timer(stmt, setup)
    unit_time = t.timeit(number=repetitions)/repetitions/relation_count
    print "Relation creation costs:   %.3f msec/relation,  %.1f rels/sec  (%d repetitions, %d relations)" % (
        1000 * unit_time,
        1./unit_time,
        repetitions, 
        relation_count)

def create_relations(n = 1):
    with db.transaction:
        s = db.node()
        t = db.node()
        for i in range(n):
            s.rel(t, **properties)


def indexing_benchmark(repetitions = 1, node_count = 1000):
    stmt = """index_nodes(%d)""" % node_count
    setup = """from __main__ import index_nodes, db"""

    t = timeit.Timer(stmt, setup)
    unit_time = t.timeit(number=repetitions)/repetitions/node_count
    print "Node indexing costs:       %.3f msec/node,      %.1f nodes/sec (%d repetitions, %d nodes)" % (
        1000 * unit_time,
        1. / unit_time,
        repetitions, 
        node_count)

def index_nodes(n=1):
    with db.transaction:
        try:
            idx = db.node.indexes.create('idx')
        except:
            idx = db.node.indexes.get('idx')

        node = db.node()

        for i in range(n):
            idx['k'][str(i)] = node


def lookup_benchmark(repetitions = 1, node_count = 1000):
    stmt = """lookup_nodes(%d)""" % node_count
    setup = """from __main__ import lookup_nodes, db"""

    t = timeit.Timer(stmt, setup)
    unit_time = t.timeit(number=repetitions)/repetitions/node_count
    print "Lookup node costs:         %.3f msec/node,      %.1f nodes/sec (%d repetitions, %d nodes)" % (
        1000 * unit_time,
        1. / unit_time,
        repetitions, 
        node_count)

def lookup_nodes(n=1):
    with db.transaction:
        idx = db.node.indexes.get('idx')

        for i in range(n):
            for node in idx['k'][str(i)]:
                break


from random import randrange
def traversal_benchmark(repetitions = 1, traversals = 1000, node_count = 50):
    # generate test network
    with db.transaction:
        nodes = [db.node() for i in range(node_count)]
        rel_count = node_count / 10
        for node in nodes:
            for i in range(rel_count):
                node.REL(nodes[randrange(node_count)])

    global start_node
    start_node = nodes[0]

    stmt = """traverse(start_node,%d)""" % traversals
    setup = """from __main__ import traverse, start_node"""

    t = timeit.Timer(stmt, setup)
    unit_time = t.timeit(number=repetitions)/repetitions/traversals
    print "Traversal costs:           %.3f msec/node,      %.1f nodes/sec (%d repetitions, %d traversals on %d nodes)" % (
        1000 * unit_time,
        1. / unit_time,
        repetitions, 
        traversals,
        node_count)

def traverse(start_node, traversals = 1000):
    for i in xrange(traversals):
        for relation in start_node.REL:
            start_node = relation.endNode
            break


if __name__ == '__main__':
    main()
