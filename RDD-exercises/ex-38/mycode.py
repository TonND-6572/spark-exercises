from pyspark.sql import SparkSession
import os

os.environ['JAVA_HOME'] = r'C:\Java\Java\jdk1.8.0_351'
os.environ['JRE_HOME'] = r'C:\Java\Java\jre1.8.0_351'

if __name__=='__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_path = os.path.join(path, 'input.csv')

    output_path = os.path.join(path, 'myoutput')

    spark = SparkSession.Builder() \
            .config('master', 'local[4]') \
            .config('spark.hadoop.fs.defaultFS', 'file:///') \
            .appName('Exercise-38') \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    rdd = sc.textFile(input_path)

    rdd_map = rdd.map(lambda x: (x.split(',')[0], float(x.split(',')[2])))

    rdd_filter = rdd_map.filter(lambda x: x[1] > 50).map(lambda x: (x[0], 1))

    rdd_reduce = rdd_filter.reduceByKey(lambda x, y: x+y).filter(lambda x: x[1] >= 2)

    print(rdd_reduce.collect())