from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

print(result)

sorted_results = collections.OrderedDict(sorted(result.items()))

print(sorted_results)
