from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession \
        .builder \
        .appName("MostPopularSuperhero") \
        .getOrCreate()

schema = StructType([StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read \
             .schema(schema) \
             .option("sep", " ") \
             .csv("Marvel+names")

lines = spark.read.text("Marvel+graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", split(col("value"), " ")[0]) \
    .withColumn("connections", size(split(col("value"), " ")) - 1) \
    .groupBy("id").agg(sum("connections").alias("connections"))

mostPopular = connections.sort(col("connections").desc()).first()

mostPopularName = names.filter(col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

spark.stop()
