from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MinTemp")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///media/user/Files/sparkCourse/DATA/1800.csv")

def parse_line(line):
    fields = line.split(",")
    station_id = fields[0]   
    entry_type  = fields[2]
    temp = float(fields[3])*0.1*(9.0/5.0)+32
    return (station_id,entry_type,temp)

rdd =lines.map(parse_line)
min_temps = rdd.filter(lambda x: "TMAX" in x[1])

station_temps= min_temps.map(lambda x: (x[0],x[2])) 

min_temps= station_temps.reduceByKey(lambda x,y: max(x,y))
results = min_temps.collect()
for i in results:
    print(i)


 

