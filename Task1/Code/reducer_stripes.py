#!/usr/bin/env python2.6

from operator import itemgetter
import sys
import ast
import operator

cat = {} #<k,v> = <movie pair, count >

current_stripe= {}
movie = None
dic_counts = {}

for line in sys.stdin:
    movie, stripe = line.split('\t', 1)
     
    stripe = ast.literal_eval(stripe)
 
    stripe = dict(stripe)
   
    #loop over the keys of the stripe
    for k in stripe:
	if int(movie) < int(k):
	        newpair  = str(movie) + ' ' + str(k)
	else:
		newpair = str(k) + ' ' + str(movie)
	if newpair in cat:
		cat[newpair] += stripe[k]
	else:
		cat[newpair] = stripe[k]

for k in cat:
	m1, m2 = k.split()
	if cat[k]>203:
		print m1, m2, cat[k]
