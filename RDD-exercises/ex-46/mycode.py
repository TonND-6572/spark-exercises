from pyspark import SparkConf, SparkContext
import os
import shutil
import argparse

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

if __name__ == '__main__':
    conf = (
        SparkConf()
            .setAppName("Exercise-42")
            .setMaster("local[4]")
            .set("spark.hadoop.fs.defaultFS", "file:///")  # Force local file system
    )

    sc = SparkContext(conf=conf).getOrCreate()

    path = os.path.dirname(os.path.abspath(__file__))

    ## input file
    input_path = os.path.join(path, 'input.txt')

    ## output file
    output = os.path.join(path, 'myoutput')
    try:
        shutil.rmtree(output)
        print('delete output file')
    except FileNotFoundError:
        print('everything ok')

    temperature = sc.textFile(input_path)

    window_size = 3
    def flat_map_func(x): # x(str, index)
        # data = x[0].split(',')
        index = x[1]

        return [(index-i, x[0]) for i in range(window_size) if index-i >= 0]

    sliding = (
        temperature.zipWithIndex()
            .flatMap(flat_map_func)
            .groupByKey().mapValues(list)
            .sortByKey()
            .map(lambda x: x[1])
            .filter(lambda x: len(x) == window_size)
            .map(lambda x: ','.join(x))
    )

    sliding.coalesce(1).saveAsTextFile(output)