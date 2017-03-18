# Mining-Massive-Datasets---MovieLens

Tech Stack: Apache Hadoop, Python, MapReduce, Spark
Libraries: Spark MLlib, Spark ML

The objective was to compare Hadoop-MapReduce and Apache Spark with their efficiency, 
to compute the frequency of co-occurrence for every pair of movies that receive a
high(4-5) ranking from the same user and then find the most frequent 20 pairs of movies.

Pairs and Stripes approach were compared (on MapReduce and Spark ) on their efficiency to compute the conditional probability. The project was enhanced to also include the computation of Lift for each of the frequent pairs of movies. [LIFT(AB)=P(AB)/(P(A)*P(B))=P(A|B)/P(A))].
