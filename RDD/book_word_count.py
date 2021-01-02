from pyspark import SparkConf,SparkContext
import re
conf = SparkConf().setMaster("local").setAppName("BookWordCount")
sc = SparkContext(conf=conf)

def parse_line(line):
    return re.compile(r'\W+',re.UNICODE).split(line.lower())
lines = sc.textFile("file:///media/user/Files/sparkCourse/DATA/book")
words = lines.flatMap(parse_line)

words_count = words.map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y)
sorted_words_count = words_count.map(lambda x : (x[1],x[0])).sortByKey(False)

results = sorted_words_count.collect()

# results = words.countByValue()

for i in results[:50]:
    # clean_word = i.encode("ascii",'ignore')
    # if (clean_word) :
    print(i[1],i[0])
