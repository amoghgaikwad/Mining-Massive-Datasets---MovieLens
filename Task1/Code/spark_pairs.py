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

    def f(x):
        line = x.strip()
            
        user, movie, rating, time = line.split('\t')
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
              return (str(sorted_movies[i]).strip(),str(sorted_movies[j]).strip())

    counts = lines.flatMap(f(x))\
                  .map(lambda x: (x, 1))\
                  .reduceByKey(lambda a, b: a+b)

    counts.pprint(filter(lambda x : x[3]>203))
    #counts.saveAsTextFile("/user/agaikwad/outputspark_pairs")
    ssc.start()
    ssc.awaitTermination()