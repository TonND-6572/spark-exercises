from pyspark.sql import SparkSession
import os

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

if __name__=='__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_path = os.path.join(path, 'input.csv')

    output_path = os.path.join(path, 'myoutput')

    spark = SparkSession.Builder() \
            .config('master', 'local[4]') \
            .config('spark.hadoop.fs.defaultFS', 'file:///') \
            .appName('Exercise-33') \
            .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    rdd = sc.textFile(input_path)

    rdd_map = rdd.map(lambda x: (x.split(',')[2], x))

    # rdd_max = rdd_map.reduce(lambda x, y: x if x > y else y)
    PM10_max = rdd_map.top(1, lambda x: x[0])[0][0]
    print(PM10_max)
    rdd_filter = rdd_map.filter(lambda x: x[0] == PM10_max)
    print(rdd_filter.collect())