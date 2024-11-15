from pyspark import SparkConf, SparkContext
import os
import argparse

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

args = argparse.ArgumentParser()
args.add_argument('-t', default=0.1, required=False, type=float)

def map_split_func(x:str, index:list):
    data = x.split(',')
    return [data[i] for i in index]

if __name__ == '__main__':
    arg = args.parse_args()

    conf = (
        SparkConf()
            .setAppName("Exercise-42")
            .setMaster("local[4]")
            .set("spark.hadoop.fs.defaultFS", "file:///")  # Force local file system
    )

    sc = SparkContext(conf=conf).getOrCreate()

    path = os.path.dirname(os.path.abspath(__file__))

    ## input file
    movies_input = os.path.join(path, 'movies.txt')
    preferences_input = os.path.join(path, 'preferences.txt')
    watchedmovies_input = os.path.join(path, 'watchedmovies.txt')

    threshold = arg.t

    ## output file
    output = os.path.join(path, 'myoutput')
    
    ## read input
    watchedmovies = sc.textFile(watchedmovies_input)
    preferences = sc.textFile(preferences_input)
    movies = sc.textFile(movies_input)

    # watchedmovies_map = watchedmovies.map(lambda x: map_split_func(x, [0, 1])) 
    users_preferences = preferences.map(lambda x: map_split_func(x, [0, 1])) # userid, genre
    movies_genre = movies.map(lambda x: map_split_func(x, [0, 2])) #moviedid, genre

    movies_genre_broadcast = sc.broadcast({k: v for k, v in movies_genre.collect()})
    users_preferences_broadcast = sc.broadcast({k: v for k, v in users_preferences.groupByKey().mapValues(list).collect()})

    def map_user_with_misleading_prof(x:str):
        data = x.split(',') # userid [0], movieid [1]

        genre_user_like = users_preferences_broadcast.value[data[0]]
        genre_of_movie = movies_genre_broadcast.value[data[1]]
        value = (0, 1) if genre_of_movie in genre_user_like else (1, 1)
        return [data[0], value]
    
    def add_custom_func(val1, val2):
        return (val1[0] + val2[0], val1[1] + val2[1])
    
    def filter_percentage_by_threshold(x):
        percentage = x[1][0] / x[1][1]
        return percentage >= float(threshold)

    watchedmovies_misleading_prof = watchedmovies.map(map_user_with_misleading_prof) \
        .reduceByKey(add_custom_func) \
        .filter(filter_percentage_by_threshold)
    
    watchedmovies_misleading_prof.coalesce(1).saveAsTextFile(output)