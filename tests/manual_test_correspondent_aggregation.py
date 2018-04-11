"""Test the signature extraction script against an annotated dataset."""
import json
import re
import collections

# directory that only contains the '*.txt.ann' files
results_path = "/Users/j/Uni/BP/code/pipeline/results_correspondent/part-00000"

agg_counter = {}

with open(results_path) as results:
    results = results.read()
    results = re.sub('\n{', ', \n{', results)
    results = '[' + results + ']'
    results = json.loads(results)

    for result in results:
        try:
            agg_counter[result['source_count']] += 1
        except Exception:
            agg_counter[result['source_count']] = 1

agg_counter_sorted = collections.OrderedDict(sorted(agg_counter.items()))

for key in agg_counter_sorted.keys():
    print(str(agg_counter_sorted[key]), 'correspondent objects were aggregated from', str(key), 'emails')
