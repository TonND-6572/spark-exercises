from pyspark.sql import SparkSession
import os
import argparse
import operator as op

os.environ['JAVA_HOME'] = 'C:\Java\jdk-11.0.16'
os.environ['JRE_HOME'] = 'C:\Java\jdk-11.0.16'

args = argparse.ArgumentParser()

args.add_argument('-k', type=int, required=False, default=1, help='number of critical sensors')

arg = args.parse_args()

def split_data(line):
    data = line.split(',')

    key = data[0]
    value = data[1] if filter_by_threshold(line) else None
    return (key, value)

def filter_by_threshold(line):
    temp = float(line.split(',')[2])

    return temp > 50

def map_values_func(data_list):
    data = list(data_list)
    
    return [x for x in data if x != None]

if __name__=='__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_path = os.path.join(path, 'input.csv')

    output_path = os.path.join(path, 'myoutput')

    k = arg.k

    spark = SparkSession.Builder() \
            .config('master', 'local[3]') \
            .config('spark.hadoop.fs.defaultFS', 'file:///') \
            .appName('Exercise-39') \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    rdd = sc.textFile(input_path)

    rdd_filter = rdd.filter(filter_by_threshold).map(lambda x: (x.split(',')[0], 1))

    rdd_reduce = rdd_filter.reduceByKey(op.add).map(lambda x: (x[1], x[0])).sortByKey(ascending=False) #.filter(lambda x: x[1] >= 2)

    print(rdd_reduce.take(k))