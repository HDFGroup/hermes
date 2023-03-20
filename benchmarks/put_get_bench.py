"""
This file is used to generate the I/O pattern of the put_get_bench

USAGE: ./put_get_bench.py [nprocs] [output]
"""

import sys, os
import yaml

nprocs = int(sys.argv[1])
output = sys.argv[2]

blobs_per_rank = 16
ranks_per_node = nprocs
bkt_name = "hello"
blob_size = 8 * (1 << 30)
ram_size = 8 * (1 << 30)
prefetch_per_rank = 1

entries = []
for rank in range(nprocs):

    # PUTS
    i = 0
    while i < blobs_per_rank:
        entries.append({
            'node_id': 0,
            'type': 1,
            'blob_name': rank * blobs_per_rank + i,
            'tag_name': bkt_name,
            'blob_size': blob_size,
            'organize_next_n': 0,
            'score': 0,
            'rank': rank
        })
        i += 1

    # GETS
    i = 0
    while i < blobs_per_rank:
        blob_name = rank * blobs_per_rank + i
        # DEMOTE THE PRIOR BLOBS AND MYSELF
        for k in range(prefetch_per_rank):
            if blob_name - k < 0:
                break
            entries.append({
                'node_id': 0,
                'type': 3,
                'blob_name': blob_name - k - 1,
                'tag_name': bkt_name,
                'blob_size': blob_size,
                'organize_next_n': 2 * prefetch_per_rank,
                'score': .5,
                'rank': rank
            })

        # PROMOTE THE NEXT BLOBS
        for k in range(prefetch_per_rank):
            if blob_name + k + 1 < 0:
                break
            entries.append({
                'node_id': 0,
                'type': 1,
                'blob_name': blob_name,
                'tag_name': bkt_name,
                'blob_size': blob_size,
                'organize_next_n': 0,
                'score': 1,
                'rank': rank
            })
            i += 1

entries = [[
    e['node_id'],
    e['type'],
    e['blob_name'],
    e['tag_name'],
    e['blob_size'],
    e['organize_next_n'],
    e['score'],
    e['rank'],
] for e in entries]

with open(output, 'w') as fp:
    yaml.dump(entries, fp)