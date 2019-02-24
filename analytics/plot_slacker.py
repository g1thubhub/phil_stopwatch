from plotly.offline import plot
from plotly.graph_objs import Figure
from typing import Dict
from parsers import ProfileParser

# job_slacker.py executed with the following command:
# ~/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.python.profile=true --conf spark.driver.extraJavaOptions=-javaagent:/Users/a/jvm-profiler/target/jvm-profiler-1.0.0.jar=sampleInterval=1000,metricInterval=100,reporter=com.uber.profiling.reporters.FileOutputReporter,outputDir=/Users/a/IdeaProjects/phil_stopwatch/analytics/data/profile_slacker  ./spark_jobs/job_slacker.py  cpumemstack /Users/a/IdeaProjects/phil_stopwatch/analytics/data/profile_slacker > JobSlacker.log

# cat data/profile_slacker/s_8_931_cpumem.json <(echo) data/profile_slacker/s_8_932_cpumem.json <(echo) data/profile_slacker/s_8_933_cpumem.json <(echo)  data/profile_slacker/CpuAndMemory.json > CombinedCpuAndMemory.json
combined_file = './data/profile_slacker/CombinedCpuAndMemory.json.gz'  # Output from JVM & PySpark profilers create by the command above

jvm_parser = ProfileParser(combined_file)
jvm_parser.manually_set_profiler('JVMProfiler')

pyspark_parser = ProfileParser(combined_file)
pyspark_parser.manually_set_profiler('pyspark')

jvm_maxima: Dict[str, float] = jvm_parser.get_maxima()
pyspark_maxima: Dict[str, float] = pyspark_parser.get_maxima()

print('JVM max values:')
print(jvm_maxima)
print('\nPySpark max values:')
print(pyspark_maxima)

# Plotting the graphs:
# Seeing all available metrices:
# print(jvm_parser.get_available_metrics())
# ['epochMillis', 'ScavengeCollTime', 'MarkSweepCollTime', 'MarkSweepCollCount', 'ScavengeCollCount', 'systemCpuLoad', 'processCpuLoad', 'nonHeapMemoryTotalUsed', 'nonHeapMemoryCommitted', 'heapMemoryTotalUsed', 'heapMemoryCommitted']
# print(pyspark_parser.get_available_metrics())
# ['epochMillis', 'pmem_rss', 'pmem_vms', 'cpu_percent']

data_points = list()
data_points.extend(jvm_parser.get_metrics(['systemCpuLoad', 'processCpuLoad']))
# Records from different profilers are in a single file so these IDs
data_points.extend(pyspark_parser.get_metrics(['cpu_percent_931'], id='931'))  # are used to collect to collect all PySpark records. They were the
                                                                               # process IDs when the code was profiled, present in every JSON records
                                                                               # outputted by PySpark profiler as value in 'pid' fields
fig = Figure(data=data_points)
plot(fig, filename='slacker-cpu.html')
