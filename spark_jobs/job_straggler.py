import os
import datetime
from sys import argv
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark_profilers import profiler_map

# Avoids this problem: 'Exception: Python in worker has different version 2.7 than that in driver 3.6',
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'


def processs(pair):
    currentlist = list()
    for i in range(pair[1]):
        currentlist.append(i)

    return currentlist


if __name__ == "__main__":
    profiler = argv[1].lower()  # cpumem
    dump_path = argv[2]  # ./ProfilePythonBusy
    print("^^ Using " + profiler + ' and writing to ' + dump_path)

    start = str(datetime.datetime.now())
    # Initialization:
    threads = 3  # program simulates a single executor with 3 cores (one local JVM with 3 threads)
    sparkContext = SparkContext('local[{}]'.format(threads), 'Profiling Straggler', profiler_cls=profiler_map[profiler])
    session = SparkSession(sparkContext)
    session.sparkContext.addPyFile('./helper.py')  # ToDo: Modify this
    session.sparkContext.addPyFile('./pyspark_profilers.py')  # ToDo: Modify this

    letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
               'v', 'w', 'x', 'y']

    frequencies = session.sparkContext.parallelize(letters) \
        .map(lambda x: (x, 1800000000) if x == "d" else (x, 30000000))

    summed = frequencies.map(processs) \
        .map(sum)

    print(summed.count())
    end = str(datetime.datetime.now())

    session.sparkContext.dump_profiles(dump_path)
    # session.sparkContext.show_profiles()  # Uncomment for printing profile records to standard out

    print("******************\n" + start + "\n******************")
    print("******************\n" + end + "\n******************")
