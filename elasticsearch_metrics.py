#! /usr/bin/env python
import argparse
from elasticsearch import Elasticsearch
import socket
import sys

PORT = 9200

METRICS = """
Supported metrics by options:
    Cluster:
        - active_primary_shards
        - active_shards
        - number_of_pending_tasks
        - relocating_shards
        - status
        - unassigned_shards
        - number_of_nodes
        - heap_max_in_bytes
        - heap_used_in_bytes
    Node:
        - heap_pool_young_gen_mem
        - heap_pool_old_gen_mem
        - heap_pool_survivor_gen_mem
        - heap_max_in_bytes
        - heap_used_in_bytes
        - heap_used_percent
        - total_filter_cache_mem
        - total_field_data_mem
        - total_merges_mem
"""

parser = argparse.ArgumentParser(description='Queries Elasticsearch for cluster/node internal metrics. Ver 1.1 beta')
parser.add_argument('-H', metavar="<host>", default="127.0.0.1", help='Hostname or IP. Default - 127.0.0.1')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-n', help='Check node metric', metavar="<metric>")
group.add_argument('-c', help='Check cluster metric', metavar="<metric>")
group.add_argument('-l', help='List all metrics', action='store_true', default=False)

args = parser.parse_args()
if args.l:
  print METRICS
  sys.exit(0)

try:
    es = Elasticsearch([{'host': args.H, 'port': PORT}], sniff_on_start=True, sniffer_timeout=5)
except Exception as e:
    print 'Connection to Elasticsearch failed!'
    sys.exit(1)

def cluster_health(metric):
    result = es.cluster.health()
    print result[metric]

def cluster_mem_stats(metric):
    result = es.cluster.stats()
    size = result['nodes']['jvm']['mem'][metric]
    print size

def node_mem_stats(metric):
    node_stats = es.nodes.stats(node_id='_local', metric='jvm')
    node_id = node_stats['nodes'].keys()[0]
    if 'heap_used_percent' in metric:
        result = node_stats['nodes'][node_id]['jvm']['mem'][metric]
        print result
    else:
        if 'pool_young' in metric:
            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['young']
            size = result['used_in_bytes']
        elif 'pool_old' in metric:
            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['old']
            size = result['used_in_bytes']
        elif 'pool_survivor' in metric:
            result = node_stats['nodes'][node_id]['jvm']['mem']['pools']['survivor']
            size = result['used_in_bytes']
        else:
            result = node_stats['nodes'][node_id]['jvm']['mem']
            size = result[metric]
        print size

def node_index_stats(metric):
    node_stats = es.nodes.stats(node_id='_local', metric='indices')
    node_id = node_stats['nodes'].keys()[0]
    if metric == 'total_merges_mem':
        result = node_stats['nodes'][node_id]['indices']['merges']
        size = result['total_size_in_bytes']
    if metric == 'total_filter_cache_mem':
        result = node_stats['nodes'][node_id]['indices']['filter_cache']
        size = result['memory_size_in_bytes']
    if metric == 'total_field_data_mem':
        result = node_stats['nodes'][node_id]['indices']['fielddata']
        size = result['memory_size_in_bytes']
    print size

cluster_checks = {'active_primary_shards': cluster_health,
                  'active_shards': cluster_health,
                  'number_of_pending_tasks': cluster_health,
                  'relocating_shards': cluster_health,
                  'status': cluster_health,
                  'unassigned_shards': cluster_health,
                  'number_of_nodes': cluster_health,
                  'heap_max_in_bytes': cluster_mem_stats,
                  'heap_used_in_bytes': cluster_mem_stats}

node_checks = {'heap_pool_young_gen_mem': node_mem_stats,
               'heap_pool_old_gen_mem': node_mem_stats,
               'heap_pool_survivor_gen_mem': node_mem_stats,
               'heap_max_in_bytes': node_mem_stats,
               'heap_used_in_bytes': node_mem_stats,
               'heap_used_percent': node_mem_stats,
               'total_filter_cache_mem': node_index_stats,
               'total_field_data_mem': node_index_stats,
               'total_merges_mem': node_index_stats}


if __name__ == '__main__':
    try:
        if args.c:
            cluster_checks.get(args.c)(args.c)
        if args.n:
            node_checks.get(args.n)(args.n)
    except TypeError:
        print("Wrong argument!")
        sys.exit(1)
