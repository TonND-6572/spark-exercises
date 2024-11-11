from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import types

from pyspark.sql import functions as f
import os

os.environ['JAVA_HOME'] = 'C:\Java\jdk-11.0.16'
os.environ['JRE_HOME'] = 'C:\Java\jdk-11.0.16'

def try_to_write_csv(df:DataFrame, output_path)->None:
    try:
        df.write.mode('overwrite').csv(output_path, header=True)
    except FileExistsError as fee:
        print('File already exists')

@f.udf(returnType=types.StringType())
def rangeage(age)->str:
    age_int = int(age)
    return f"[{(age_int//10)*10}-{(age_int//10)*10+9}]"

def by_dataframe(df:DataFrame, output_path) -> DataFrame:
    new_df = df.withColumn('name_surname', f.concat(df['name'], f.lit('_'), df['surname']))

    try_to_write_csv(new_df.select('name_surname'), output_path)

def by_sql(session:SparkSession, df: DataFrame, output_path) -> DataFrame:
    df.createOrReplaceTempView('users')
    query = """
        SELECT 
            CONCAT(name,"_",surname) AS name_surname
        FROM 
            users
    """
    df_2 = session.sql(query)
    try_to_write_csv(df_2, output_path)

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