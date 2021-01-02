from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSql").getOrCreate()
sc = spark.sparkContext


def mapper(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    ID = int(fields[0])
    name = str(fields[1]).encode("utf-8").decode('utf-8')
    return Row(
        age=age,
        numFriends=numFriends,
        ID=ID,
        name=name)


lines = sc.textFile(
    "file:///media/user/Files/sparkCourse/DATA/fakefriends.csv")

people = lines.map(mapper)
schemapeople = spark.createDataFrame(people).cache()
schemapeople.createOrReplaceTempView("people")


teenagers = spark.sql("select * from people where age >13 order by age desc")
for teen in teenagers.collect():
    print(teen)


schemapeople.groupby("age").count().orderBy("age").show()