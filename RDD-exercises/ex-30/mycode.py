from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession
import os
from pathlib import Path

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

if __name__ == '__main__':
    #current path
    path = os.path.dirname(os.path.abspath(__file__))
    #get input
    input_path = os.path.join(path, 'input.txt')

    #get output
    output_path = os.path.join(path, 'myoutput')

    conf = (
        SparkConf()
        .setAppName("Exercise-30")
        .setMaster("local[*]")
        .set("spark.hadoop.fs.defaultFS", "file:///")  # Force local file system
    )

    sc = SparkContext(conf=conf)
    sc.setLogLevel('ERROR')

    rdd_df = sc.textFile(input_path)

    rdd_line_map = rdd_df.filter(lambda x: 'google' in x)
    
    
    rdd_line_map.saveAsTextFile(output_path)
