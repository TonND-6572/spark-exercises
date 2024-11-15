from pyspark import SparkContext, SparkConf
import operator as op
import os
import argparse

os.environ['JAVA_HOME'] = 'C:\\Java\\Java\\jdk1.8.0_351'
os.environ['JRE_HOME'] = 'C:\\Java\\Java\\jre1.8.0_351'

args = argparse.ArgumentParser()

args.add_argument('-t', default=3, help='Threshold for critical situations', type=int)

def is_critical(free_slot, threshold):
    return (1, 1) if int(free_slot) < int(threshold) else (0, 1) # number of free slot is below provide threshold

def reading_map_critical_func(x:str, threshold:int)->list:
    data = x.split(',')
    free_slot = data[5]
    return [data[0], is_critical(free_slot, threshold)]

def mapping_hour(x, threshold):
    data = x.split(',')
    min_hour = 4 * (int(data[2]) // 4)
    max_hour = min_hour + 3
    free_slot = data[5]
    return [f'{data[0]}_[{min_hour}-{max_hour}]', is_critical(free_slot, threshold)]

def mapping_timestamp_func(x):
    data = x.split(',')
    time_stamp = f'{data[1]} {data[2]}:{data[3]}' 
    return [time_stamp, x]

def neighbor_map_func(x:str):
    data = x.split(',')
    return [data[0], tuple(data[1].split(' '))]

def zero_slot_filter(x):
    data = x.split(',')
    free_slot = int(data[5])
    return free_slot == 0

def add_func(val1, val2):
    return (val1[0] + val2[0], val1[1] + val2[1])

def percentage_map_func(x):
    return (x[0], x[1][0] / x[1][1])

if __name__ == '__main__':
    arg = args.parse_args()
    threshold = arg.t
    
    conf = (
        SparkConf()
            .setAppName("Exercise-42")
            .setMaster("local[4]")
            .set("spark.hadoop.fs.defaultFS", "file:///")  # Force local file system
    )

    sc = SparkContext(conf=conf).getOrCreate()

    path = os.path.dirname(os.path.abspath(__file__))

    # input
    neighbor_input = os.path.join(path, 'neighbors.txt')
    reading_input = os.path.join(path, 'readings.txt')

    #output
    percentage_critical_situations_output = os.path.join(path, 'percentage_critical_situations')
    percentage_critical_situations_ts_output = os.path.join(path, 'percentage_critical_situations_ts')
    station_full_output = os.path.join(path, 'station_full')

    neighbor = sc.textFile(neighbor_input)
    reading = sc.textFile(reading_input).cache()

    ## Part-1: percentage of critical situations
    station_count_pair_rdd = reading.map(lambda x: reading_map_critical_func(x, threshold))

    station_total_count_pair = station_count_pair_rdd.reduceByKey(add_func)
    
    station_percentage_pair = station_total_count_pair.map(percentage_map_func) \
            .filter(lambda x: x[1] >= 0.8).sortBy(lambda x: x[1], ascending=False)

    station_percentage_pair.coalesce(1).saveAsTextFile(percentage_critical_situations_output)
    
    ## Part-2: 
    station_timeslot_rdd = reading.map(lambda x: mapping_hour(x, threshold))

    station_timeslot_percentage = station_timeslot_rdd.reduceByKey(add_func) \
                    .map(percentage_map_func) \
                    .filter(lambda x: x[1] > 0.8).sortBy(lambda x: x[1], ascending=False)
    
    station_timeslot_percentage.coalesce(1).saveAsTextFile(percentage_critical_situations_ts_output)

    ## Part-3
    zero_slot_station = reading.filter(zero_slot_filter).map(mapping_timestamp_func)
    zero_slot_group_by_timestamp = zero_slot_station.groupByKey().mapValues(list)

    neighbor_map = neighbor.map(neighbor_map_func)
    
    neighbors = {k: v for k, v in neighbor_map.collect()}
    neighbor_broadcast = sc.broadcast(neighbors)

    def filter_func(data:list, neighbor_broadcast):
        stations = []
        for line in data:
            stations.append(line.split(',')[0])

        for station in stations:
            for neighbor in neighbor_broadcast.value[station]:
                if neighbor not in stations: return False
            
        return True

    full_stations = zero_slot_group_by_timestamp.filter(lambda x: filter_func(x[1], neighbor_broadcast))

    full_stations.coalesce(1).saveAsTextFile(station_full_output)