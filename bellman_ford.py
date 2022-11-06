from typing import Tuple, List
from math import log
from math import exp
from datetime import datetime

edges = []
currencies = {} 
graph = {}

def negate_logarithm_convertor(graph: Tuple[Tuple[float]]) -> List[List[float]]:
    ''' log of each rate in graph and negate it'''
    result = []
    for row in graph:
        result.append([row[0], row[1], row[2]])
    return result


def arbitrage(source, currencies, tolerance):
    ''' Calculates arbitrage situations and prints out the details of this calculations'''

    # Pick any source vertex -- we can run Bellman-Ford from any vertex and get the right result
    n = len(currencies)
    
    min_dist = {}
    for curr in currencies:
        min_dist[curr] = float("inf")
    
    min_dist[source] = 0

    pre = {}

    # 'Relax edges |V-1| times'
    for _ in range(n-1):
        for u,v,w,_,_ in edges:
            if min_dist[u] != float('inf') and min_dist[u] + w < min_dist[v]:
                min_dist[v] = min_dist[u] + w
                pre[v] = u

    print_cycle = None

    # if we can still relax edges, then we have a negative cycle
    for source_curr, dest_curr, w, rate, _ in edges:
        if min_dist[source_curr] != float('inf') and min_dist[dest_curr] - tolerance > min_dist[source_curr] + w:
            # negative cycle exists, and use the predecessor chain to print the cycle
            print_cycle = [dest_curr, source_curr]
            
            while pre[source_curr] not in print_cycle:
                print_cycle.append(pre[source_curr])
                source_curr = pre[source_curr]
            print_cycle.append(pre[source_curr])
            print_cycle.reverse()
            #return print_cycle[print_cycle.index(pre[source_curr]):]
            return print_cycle[:print_cycle.index(pre[source_curr], 1)+1]

    return print_cycle

def add_edge(edge):
    edges.append([edge[1], edge[2], -log(edge[3]), edge[3], edge[0]])
    edges.append([edge[2], edge[1], -log(1/edge[3]), 1/edge[3], edge[0]])
    # need the timestamp to validate edges less than 1.5 seconds old

def validate_edges():
    source = None
    for edge in edges:
        edge_time = edge[4]
        curr_time = datetime.now().timestamp()
        if curr_time - edge_time < 1.5:
            source = edge[0]
        else:
            edges.remove(edge)

    currencies = set()
    for edge in edges:
        src, dest = edge[0], edge[1]
        currencies.add(src)
        currencies.add(dest)
        if src not in graph:
            graph[src] = {}
        #graph[src][dest] = edge[2]
        graph[src][dest] = edge[3]

    return source, currencies

def friendly_output(pc):
    print("\nArbitrage Opportunity: \n")
    print("start with 100 of {}".format(pc[0]))
    rate = 100
    for i, value in enumerate(pc):
        if i + 1 < len(pc):
            #rate *= exp(-graph[pc[i]][pc[i+1]])
            rate = rate * graph[pc[i]][pc[i+1]]
            print("exchange {} for {} at {} --> {} ".format(pc[i], pc[i+1], graph[pc[i]][pc[i+1]], rate))

def run(edge):
    add_edge(edge)
    source, currencies = validate_edges()
    if source != None:
        pc = arbitrage(source, currencies, 0.002)
        if pc != None:
            friendly_output(pc)
    graph.clear()
