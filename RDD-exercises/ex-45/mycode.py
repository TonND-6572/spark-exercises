from pyspark import SparkConf, SparkContext
import os
import shutil
import argparse

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

def map_split_func(x:str, index:list):
    data = x.split(',')
    return [data[i] for i in index]

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
    movies_input = os.path.join(path, 'movies.txt')
    preferences_input = os.path.join(path, 'preferences.txt')
    watchedmovies_input = os.path.join(path, 'watchedmovies.txt')

    ## output file
    output = os.path.join(path, 'myoutput')
    try:
        shutil.rmtree(output)
        print('delete output file')
    except FileNotFoundError:
        print('everything ok')

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

        return [data[0], genre_of_movie if genre_of_movie not in genre_user_like else None] 
    
    watchedmovies_misleading_prof = watchedmovies.map(map_user_with_misleading_prof).filter(lambda x: x[1] is not None).groupByKey().mapValues(list)
    
    watchedmovies_misleading_prof.coalesce(1).saveAsTextFile(output)