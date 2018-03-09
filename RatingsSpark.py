from pyspark.sql import SparkConf, SparkContext
def loadMovieNames():
	movieNames={}
	with open("ml-100k/u.item") as f:
		for line in f:
			fields=line.split('|')
			movieNames[int(fields[0])]=fields[1]
	return movieNames

def parseInput(line):
	fields-line.value.split()
	return (int(fields[1],(float(fields[2]),1.0))
if __name__=="__main__":
	conf=SpaprkConf().setAppName("WprstMovies")
	sc=SparkContext(conf=conf)
	movieNames=loadMovieNames()
	lines=sc.textFile("hdfs://user/rohit_gop/ml-100k/u.data")
	movieRatings=lines.map(parseInput)
	ratingTotalsAndCount=movieRatings.redcueByKey(lambda mvoie1,movie2:(movie1[0]+movie2[0]))
	averageRatings=ratingTotalsAndCount.mapValues(lambda totalAndCount:totalAndCount[0]/totalAndCount[1])
	sortedMovies=averageRatings.sortBy(lambda x:x[1])
	results=sortedMovies.take(10)
	for result in results:
		print(movieNames[result[0]],result[1])
