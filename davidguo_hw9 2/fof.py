#####FIXED

from pyspark import SparkConf, SparkContext
import sys
import itertools
if len(sys.argv) != 3:
    print('Usage: ' + sys.argv[0] + ' <in><out>')
    sys.exit(1)
inputlocation = sys.argv[1]
outputlocation = sys.argv[2]


conf = SparkConf().setAppName('Triangle')
sc = SparkContext(conf = conf)

#functions
def conv_int(l):
    return [int(x) for x in l]

# order as a tuple, could use lambda x: tuple(sorted(x))
def rev(y):
    if y[0] > y[1]:
        return y[1], y[0]
    else:
        return y

#    
data = sc.textFile(inputlocation) # input: 'hdfs:///var/stat701w18/fof/friends.simple'
#change to unicode  #https://stackoverflow.com/questions/34479444/how-to-remove-unicode-when-reading-data
data = data.map(lambda l: l.encode('ascii')) 
data = data.map(lambda l: l.split())
data = data.map(conv_int) #convert string to int

data = data.map(lambda v: (v[0], v[1:])) 

data = data.map(lambda v: (v[0], list(itertools.combinations(v[1],2))))# first user as key, rest as values

#flatmapvalues http://spark.apache.org/docs/2.1.0/api/python/pyspark.html
#generate all pairs from key and the list of values

data = data.flatMapValues(lambda x: x).map(lambda x: (x[0], x[1][0], x[1][1])).map(lambda x: tuple(sorted(x))).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] > 1).keys()
data.saveAsTextFile(outputlocation)
sc.stop()

#https://stackoverflow.com/questions/11304895/how-to-scp-a-folder-from-remote-to-local


# scp fof.py davidguo@flux-hadoop-login.arc-ts.umich.edu:fof.py
# spark-submit --master yarn --queue teaching fof.py hdfs:///var/stat701w18/fof/friends.simple/ hdfs:///user/davidguo/no3
# hdfs dfs -get hdfs:///user/davidguo/no3
# scp -r davidguo@flux-hadoop-login.arc-ts.umich.edu:/home/davidguo/no3/ small_triangle_list_parts/

# spark-submit --master yarn --queue teaching --num-executors 35 --executor-memory 5g --executor-cores 4 fof.py hdfs:///var/stat701w18/fof/friends1000 hdfs:///user/davidguo/no3large
# hdfs dfs -get hdfs:///user/davidguo/no3large
# scp -r davidguo@flux-hadoop-login.arc-ts.umich.edu:/home/davidguo/no3large/ large_triangle_list_parts/
