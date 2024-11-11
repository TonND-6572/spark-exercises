from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from pyspark.sql import functions as f
import os

os.environ['JAVA_HOME'] = 'C:\Java\jdk-11.0.16'
os.environ['JRE_HOME'] = 'C:\Java\jdk-11.0.16'

def try_to_write_csv(df:DataFrame, output_path)->None:
    try:
        df.write.csv(output_path, header=False)
    except FileExistsError as fee:
        print('File already exists')

def by_dataframe(df:DataFrame, output_path) -> DataFrame:
    male = df.groupBy('name').agg(
        f.count('age').alias('number_of_name'),
        f.avg('age').alias('avg_age')
    ).filter("number_of_name > 1").select('name', 'avg_age').orderBy('avg_age', ascending=False)
    
    # print(male.show())
    
    try_to_write_csv(male, output_path)
    print('Write with dataframe success')

def by_sql(session:SparkSession, df: DataFrame, output_path) -> DataFrame:
    df.createOrReplaceTempView('users')
    query = """
        SELECT 
            name, AVG(age)
        FROM 
            users
        GROUP BY name
        HAVING COUNT(age) > 1 
    """
    df_2 = session.sql(query)
    try_to_write_csv(df_2, output_path)
    print('write to sql dataframe success')

if __name__ == '__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_file = os.path.join(path, 'input.csv')

    spark = SparkSession.Builder() \
        .config('master', 'local[3]') \
        .config('spark.hadoop.fs.defaultFS', 'file:///') \
        .appName('Exercise-48').getOrCreate()
    
    df = spark.read.csv(input_file, header=True)
    
    by_sql(spark, df, os.path.join(path, 'sql_output'))
    by_dataframe(df, os.path.join(path, 'df_output'))