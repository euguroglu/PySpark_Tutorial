from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    customer = int(fields[0])
    spend = float(fields[2])
    return (customer, spend)

lines = sc.textFile("C:/Users/PC/Documents/Jupyter/Udemy/Spark_Frank_Kane/customer-orders.csv")
rdd = lines.map(parseLine)
total_spend = rdd.reduceByKey(lambda x, y : x + y)
total_spend_sorted = total_spend.map(lambda x:(x[1], x[0])).sortByKey()
result = total_spend_sorted.collect()
for result in result:
    print(result)
