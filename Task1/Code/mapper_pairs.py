#!/usr/bin/env python

import sys
import os
from collections import defaultdict

movie_list = defaultdict(list) #dict <k,v> = <userid, [list of movies]>


for line in sys.stdin:
    line = line.strip()
	
    #for the 1M and 10M datasets the delimiter chges to "::"
    user, movie, rating, time = line.split('::')    
    #user, movie, rating, time = line.split('\t')
    user = user.strip()
    movie = movie.strip()
    rating = rating.strip()
    user = int(user)
    movie = int(movie)
    try:
        rating = int(rating)
    except ValueError:
        continue        
   
    #skip movies with ratings < 4 
    if rating < 4:
    	continue    
    #Build dictionary
    else:
        if user in  movie_list.keys():
            if movie in  movie_list[user]:
                continue
            else:
                 movie_list[user].append(movie)
        else:
             movie_list[user].append(movie)


for user in  movie_list.keys():
    if len( movie_list[user]) > 1:
        movies = list( movie_list[user])
	movies = map(int, movies)
        sorted_movies = sorted(movies)
	for i in xrange(0,len(sorted_movies)):
		for j in xrange (i+1,len(sorted_movies)): 
		    #Emit Pairs
		    print str(sorted_movies[i]).strip(),str(sorted_movies[j]).strip(),'\t', '1'
