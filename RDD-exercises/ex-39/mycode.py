from pyspark.sql import SparkSession
import os

os.environ['JAVA_HOME'] = 'C:\Java\jdk-11.0.16'
os.environ['JRE_HOME'] = 'C:\Java\jdk-11.0.16'

def split_data(line):
    split_df = line.split(',')

    return (split_df[0], split_df[1])

def filter_by_threshold(line):
    temp = float(line.split(',')[2])

    return temp > 50

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

    rdd_filter = rdd.filter(filter_by_threshold).map(split_data)

    rdd_reduce = rdd_filter.groupByKey().mapValues(list).collect() #.filter(lambda x: x[1] >= 2)

    print(rdd_reduce)