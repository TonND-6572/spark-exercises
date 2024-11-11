from pyspark.sql import SparkSession
import os

os.environ['JAVA_HOME'] = 'C:\Java\jdk-11.0.16'
os.environ['JRE_HOME'] = 'C:\Java\jdk-11.0.16'

def split_data(line):
    data = line.split(',')

    key = data[0]
    value = data[1] if filter_by_threshold(line) else None
    return (key, value)

def filter_by_threshold(line):
    temp = float(line.split(',')[2])

    return temp > 50

def swap(line):
    return (line[1], line[0])

if __name__=='__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_path = os.path.join(path, 'input.csv')

    output_path = os.path.join(path, 'myoutput')

    spark = SparkSession.Builder() \
            .config('master', 'local[3]') \
            .config('spark.hadoop.fs.defaultFS', 'file:///') \
            .appName('Exercise-39') \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    rdd = sc.textFile(input_path)

    rdd_filter = rdd.filter(filter_by_threshold).map(lambda x: (x.split(',')[0], 1))

    # rdd_reduce = rdd_filter.groupByKey().mapValues(map_values_func) #.filter(lambda x: x[1] >= 2)
    rdd_reduce = rdd_filter.reduceByKey(lambda x, y: x + y).map(swap).sortByKey(ascending=False)
    print(rdd_reduce.collect())