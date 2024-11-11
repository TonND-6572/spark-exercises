from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max
import os

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

def by_rdd(sc: SparkContext, input_path):
    rdd = sc.textFile(input_path)

    rdd_line_map = rdd.map(lambda x: x.split(',')[2])

    rdd_max = rdd_line_map.reduce(lambda x, y: x if x > y else y)

    return rdd_max

def by_spark_df(spark: SparkSession, input_path):
    df = spark.read.csv(input_path)

    return df.agg(spark_max('_c2')).collect()[0][0]

if __name__=='__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_path = os.path.join(path, 'input.csv')

    output_path = os.path.join(path, 'myoutput')

    spark = SparkSession.builder \
            .config('master', 'local[4]') \
            .config('spark.hadoop.fs.defaultFS', 'file:///') \
            .appName('Exercise-32') \
            .getOrCreate()

    sc = spark.sparkContext
    
    # print(by_rdd(sc, input_path))
    print(by_spark_df(spark, input_path))

    