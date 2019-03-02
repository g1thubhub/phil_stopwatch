import os
import datetime
from sys import argv
from pyspark.sql import SparkSession
from pyspark import SparkContext
from helper import fat_function_inner
from pyspark_profilers import profiler_map

# Avoids this problem: 'Exception: Python in worker has different version 2.7 than that in driver 3.6',
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'  # ToDo: Modify this
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'  # ToDo: Modify this


def fat_function_outer(string):
    result = ''
    for i in range(0, 100000):
        if i % 10000 == 0:
            i = 0
        result = fat_function_inner(i)
    return string + '@@' + str(result[-1])


if __name__ == '__main__':
    profiler = argv[1].lower()  # cpumem
    dump_path = argv[2]  # ./ProfilePythonBusy
    print("^^ Using " + profiler + ' and writing to ' + dump_path)

    start = str(datetime.datetime.now())
    # Initialization:
    threads = 3  # program simulates a single executor with 3 cores (one local JVM with 3 threads)
    # conf = (SparkConf().set('spark.python.profile', 'true'))
    sparkContext = SparkContext('local[{}]'.format(threads), 'Profiling Busy', profiler_cls=profiler_map[profiler])
    session = SparkSession(sparkContext)
    session.sparkContext.addPyFile('./helper.py')  # ToDo: Modify this
    session.sparkContext.addPyFile('./pyspark_profilers.py')  # ToDo: Modify this

    records = session.createDataFrame([('a',), ('b',), ('c',)])
    result = records.rdd.map(lambda x: fat_function_outer(x[0]))
    print("@@@ " + str(result.collect()))
    end = str(datetime.datetime.now())

    session.sparkContext.dump_profiles(dump_path)
    # session.sparkContext.show_profiles()  # Uncomment for printing profile records to standard out

    print("******************\n" + start + "\n******************")
    print("******************\n" + end + "\n******************")
