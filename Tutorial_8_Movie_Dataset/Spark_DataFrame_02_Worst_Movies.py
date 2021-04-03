from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    #Create a  SparkSession
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    #Load up our movie ID -> name dictionary

    movieNames = loadMovieNames

    #Get the raw data
    lines = spark.sparkContext.textFile("u.data")

    #Convert it to a RDD of row object with (movieID, rating)
    movies = lines.map(parseInput)

    #Convert that to a dataframe
    movieDataset = spark.createDataFrame(movies)

    #Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    #Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    #Join the two together (we now have movieID, avg(rating) and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    #Filter count less than 10
    averagesAndCounts = averagesAndCounts.filter("count<10")
    #Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)")

    #Print them out converting movieID's to names as we go
    # for movie in topTen:
    #     print(movieNames[movie[0]], movie[1], movie[2])
    topTen.show()
    #Stop the SparkSession
    spark.stop()
