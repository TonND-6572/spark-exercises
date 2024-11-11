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

    rdd = sc.textFile(input_path)

    rdd_map_line = rdd.map(lambda x: x.split(',')[2])

    rdd_sort = rdd_map_line.sortBy(lambda x: x, ascending=False)

    sc.parallelize(rdd_sort.take(3)).coalesce(1).saveAsTextFile(output_path)