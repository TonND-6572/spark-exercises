from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os

os.environ['JAVA_HOME'] = 'C:\Java\jdk-11.0.16'
os.environ['JRE_HOME'] = 'C:\Java\jdk-11.0.16'

def by_dataframe(df:DataFrame, output_path) -> DataFrame:
    male = df.filter(df.gender == 'male') \
            .select('name', 'age') \
            .withColumn('age', df.age + 1) \
            .orderBy(['age', 'name'], ascending=[False, True])
    
    male.write.csv(output_path, header=False)
    print('Write with dataframe success')

def by_sql(session:SparkSession, df: DataFrame, output_path) -> DataFrame:
    df.createOrReplaceTempView('users')

    df_2 = session.sql('SELECT name, age + 1 FROM users WHERE gender == "male" ORDER BY AGE DESC, NAME ASC;')

    df_2.write.csv(output_path, header=False)

if __name__ == '__main__':
    path = os.path.dirname(os.path.abspath(__file__))

    input_file = os.path.join(path, 'input.csv')

    spark = SparkSession.Builder() \
        .config('master', 'local[3]') \
        .config('spark.hadoop.fs.defaultFS', 'file:///') \
        .appName('Exercise-39').getOrCreate()
    
    df = spark.read.csv(input_file, header=True)
    
    by_sql(spark, df, os.path.join(path, 'sql_output'))
    by_dataframe(df, os.path.join(path, 'df_output'))
    