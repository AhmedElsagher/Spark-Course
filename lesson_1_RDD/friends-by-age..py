from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

lines = sc.textFile(
    "file:///media/user/Files/sparkCourse/DATA/fakefriends.csv")


def pase_line(line):
    words = line.split(",")
    age = int(words[2])
    numFriends = int(words[3])
    return (age, numFriends)


rdd = lines.map(pase_line)
totalByAge = rdd.mapValues(lambda x: (x, 1)).\
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
totalByAge = totalByAge.sortByKey()
averageByAge = totalByAge.mapValues(lambda x: int(x[0]/x[1]))
results = averageByAge.collect()
for i in results:
    print(i)
