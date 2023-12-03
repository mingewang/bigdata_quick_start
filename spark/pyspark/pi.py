import sys
from random import random
from operator import add
from pyspark.sql import SparkSession
#
if __name__ == "__main__":
  """
  Usage: pi [partitions]
  """
  spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()
  # partitions: divide the job number of partitions, 
  # which will be schedule to run on multiple spark executors in the cluster
  partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
  n = 100000 * partitions
  #
  def f(_: int) -> float:
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0
  # 
  # parallelize here means: split n+1 computes into partitions,
  # so it can be be run on partitions nodes 
  # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.parallelize.html
  #
  # map(f): means applying a function to each element of this RDD
  # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.map.html
# #
  # reduce(add) means: Reduces the elements of this RDD using the specified commutative and associative binary operator.
  # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduce.html
  #
  count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
  # get Pi according to ration
  print("Pi is roughly %f" % (4.0 * count / n), " using ",  partitions, " partitions")
#
spark.stop()
