from pyspark import SparkContext, SparkConf
import os

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

if __name__=='__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_path = os.path.join(path, 'input.txt')

    output_path = os.path.join(path, 'myoutput')

    conf = (
        SparkConf()
        .setAppName('Exercise-31')
        .setMaster('local[4]')
        .set("spark.hadoop.fs.defaultFS", "file:///")  # Force local file system
        .set('logLevel', 'ERROR')
    )

    sc = SparkContext(conf=conf)
    
    rdd = sc.textFile(input_path)

    rdd_filter = rdd.filter(lambda x: 'www.google.com' in x)

    rdd_map_ip = rdd_filter.map(lambda x: x.split('--')[0])

    rdd_map_ip.distinct().coalesce(1).saveAsTextFile(output_path)   