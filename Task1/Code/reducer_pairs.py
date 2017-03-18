#!/usr/bin/env python


from operator import itemgetter
import sys

current_pair = None
current_count = 0
pair = None

for line in sys.stdin:
    line = line.strip()

    pair, count = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:
        continue

    if current_pair == pair:
        current_count += count
    else:
        if current_pair:
                print current_pair, current_count
        current_count = count
        current_pair = pair

#output the last pair
if current_pair == pair:
    if current_count>203:
       print current_pair,current_count