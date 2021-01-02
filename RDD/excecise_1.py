from pyspark import SparkConf,SparkContext


conf = SparkConf().setMaster("local").setAppName("BookWordCount")
sc = SparkContext(conf=conf)

def parse_lines(line):
    list_details = line.split(",")
    customer_id = list_details[0]
    price = float(list_details[2])
    return (customer_id,price)
    
customer_data = sc.textFile("/media/user/Files/sparkCourse/DATA/customer-orders.xls")
customer_data = customer_data.map(parse_lines).reduceByKey( lambda x,y:x+y)
customer_data = customer_data.map(lambda x:(x[1],x[0])).sortByKey( False)
results = customer_data.collect()


for i in results[:50]:
    # clean_word = i.encode("ascii",'ignore')
    # if (clean_word) :
    print(i[1],round(i[0],2))
