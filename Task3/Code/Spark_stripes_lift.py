from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreaming")
    ssc = StreamingContext(sc, 1)

    lines = ssc.textFileStream(sys.argv[1])

    def S(x):
              line = x.strip()
            
              user, movie, rating, time = line.split('\t')
              user = int(user)
              movie = int(movie)    
              rating = int(rating)
              if rating < 4:
                continue    

              if user in favorites.keys():
                if movie in favorites[user]:
              continue
            else:
                    favorites[user].append(movie) 
              else:
                favorites[user].append(movie)

          for user in favorites.keys():
              if len(favorites[user]) > 1:
                  movies = list(favorites[user])
            movies = map(int, movies)
                  sorted_movies = sorted(movies)
            for i in xrange(0,len(sorted_movies)):
              for j in xrange (i+1,len(sorted_movies)): 
                   if int(sorted_movies[i]) < int(sorted_movies[j]):
                    pair = str(sorted_movies[i]).strip(), str(sorted_movies[j]).strip() 
                   else:
                    pair = str(sorted_movies[j]).strip(), str(sorted_movies[i]).strip() 
                              if pair in pairs:
                                  pairs[pair]+=1
                              else:
                                  pairs[pair]= 1
          for pair  in pairs:
            m1, m2 = pair[0], pair[1]
            dict_stripe[m1][m2]= pairs[pair]
          for k in dict_stripe:
            return (str(k),str(dict_stripe[k]))
    
    def R(k):
            movie, stripe = line.split('\t', 1)
            stripe = ast.literal_eval(stripe)
            stripe = dict(stripe)
            for k in stripe:
            if int(movie) < int(k):
                    newpair  = str(movie) + ' ' + str(k)
            else:
              newpair = str(k) + ' ' + str(movie)
              return newpair

    counts = lines.flatMap(S(x))\
                  .map(lambda k: x[1])\
                  .flatMap(R(k)).map(lambda m: (tuple(m), 1))\
                  .reduceByKey(lambda x,y : x+y)
    t_count= counts.count()
    cprobability = counts.filter(lambda (x, v): x == v).count() / t_count.count()
    output_lift = cprobability.map(cprobability.split(',')).filter(lambda x:x[1]).count()
                  

    counts.pprint()
    #counts.take(20) # to get the top 20 movies
    #counts.saveAsTextFile("/user/agaikwad/outputspark_stripes")
    ssc.start()
    ssc.awaitTermination()