from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os 

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

def split_func(x:str, index:list):
    data = x.split(',')
    return [data[i] for i in index]

if __name__ == '__main__':
    conf = (
        SparkConf()
            .setAppName("Exercise-42")
            .setMaster("local[4]")
            .set("spark.hadoop.fs.defaultFS", "file:///")  # Force local file system
    )
    
    abs_path = os.path.dirname(os.path.abspath(__file__))

    questions_path = os.path.join(abs_path, 'questions.txt')
    answers_path = os.path.join(abs_path, 'answers.txt')

    output_path = os.path.join(abs_path, 'myoutput')

    # sc = SparkContext(conf=conf).getOrCreate()
    spark = SparkSession.Builder() \
            .config('master', 'local[3]') \
            .config('spark.hadoop.fs.defaultFS', 'file:///') \
            .appName('Exercise-39') \
            .getOrCreate()
    
    sc = spark.sparkContext

    questions = sc.textFile(questions_path)
    answers = sc.textFile(answers_path)

    questions_map = questions.map(lambda x: split_func(x, [0, 2]))
    answers_map = answers.map(lambda x: split_func(x, [1, 3]))

    questions_group = questions_map.groupByKey().mapValues(list)
    answers_group = answers_map.groupByKey().mapValues(list)

    questions_group.join(answers_group).coalesce(1).saveAsTextFile(output_path)