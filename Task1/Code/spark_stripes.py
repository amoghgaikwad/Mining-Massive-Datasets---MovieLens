"""
 Counts words in new text files created in the given directory
 Usage: hdfs_wordcount.py <directory>
   <directory> is the directory that Spark Streaming will use to find and read new text files.

 To run this on your local machine on directory `localdir`, run this example
    $ bin/spark-submit examples/src/main/python/streaming/hdfs_wordcount.py localdir

 Then create a text file in `localdir` and the words in the file will get counted.
"""
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

              #skip movies with ratings < 4 
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
              #only considered users that have rated atleast two movies
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
                   #count pairs
                               if pair in pairs:
                pairs[pair]+=1
                   else:
                                  pairs[pair]= 1

          # Make stripes
          for pair  in pairs:
            m1, m2 = pair[0], pair[1]
            dict_stripe[m1][m2]= pairs[pair]

          #Emit stripes
          for k in dict_stripe:
            return (str(k),str(dict_stripe[k]))
    
    def R(k):
            movie, stripe = line.split('\t', 1)
             
            stripe = ast.literal_eval(stripe)
         
            stripe = dict(stripe)
           
            #loop over the keys of the stripe
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

    counts.pprint(filter(lambda x : x[3]>203))
    #counts.take(20)
    #counts.saveAsTextFile("/user/agaikwad/outputspark_stripes")
    ssc.start()
    ssc.awaitTermination()