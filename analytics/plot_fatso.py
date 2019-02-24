from plotly.offline import plot
from parsers import ProfileParser, SparkLogParser
from plotly.graph_objs import Data, Figure, Layout
from helper import get_max_y


# The two files used below are created by running
#  ~/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --class profile.sparkjobs.JobFatso --conf spark.driver.extraJavaOptions=-javaagent:/Users/phil/jvm-profiler/target/jvm-profiler-1.0.0.jar=sampleInterval=100,metricInterval=100,reporter=com.uber.profiling.reporters.FileOutputReporter,outputDir=./ProfileFatso   target/scala-2.11/philstopwatch-assembly-0.1.jar > JobFatso.log

profile_file = './data/ProfileFatso/CpuAndMemoryFatso.json.gz'  # Output from JVM profiler
profile_parser = ProfileParser(profile_file, normalize=True)
data_points = profile_parser.make_graph()


logfile = './data/ProfileFatso/JobFatso.log.gz'
log_parser = SparkLogParser(logfile)
stage_interval_markers = log_parser.extract_stage_markers()
data_points.append(stage_interval_markers)

layout = log_parser.extract_job_markers(700)
fig = Figure(data=data_points, layout=layout)
plot(fig, filename='fatso.html')


#################################################################################################

# Profiling PySpark & JVM:
# Run with
# ~/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.python.profile=true --conf spark.driver.extraJavaOptions=-javaagent:/Users/phil/jvm-profiler/target/jvm-profiler-1.0.0.jar=sampleInterval=100,metricInterval=100,reporter=com.uber.profiling.reporters.FileOutputReporter,outputDir=/Users/phil/IdeaProjects/phil_stopwatch/analytics/data/profile_fatso  ./spark_jobs/job_fatso.py cpumemstack /Users/phil/IdeaProjects/phil_stopwatch/analytics/data/profile_fatso > Fatso_PySpark.log
# 
# Easiest way is to concatenate all JVM & PySpark profiles into a single file first via
# cat data/profile_fatso/s_8_7510_cpumem.json <(echo) data/profile_fatso/s_8_7511_cpumem.json <(echo) data/profile_fatso/s_8_7512_cpumem.json <(echo) data/profile_fatso/CpuAndMemory.json > data/profile_fatso/CombinedProfile.json
# ...and then applying some manual settings:

data_points = list()
combined_file = './data/profile_fatso/CombinedProfile.json'

jvm_parser = ProfileParser(combined_file)
jvm_parser.manually_set_profiler('JVMProfiler')
data_points.extend(jvm_parser.make_graph())

pyspark_parser = ProfileParser(combined_file)
pyspark_parser.manually_set_profiler('pyspark')
                                                          # Records from different profilers are in a single file so these IDs
data_points.extend(pyspark_parser.make_graph(id='7510'))  # are used to collect to collect all PySpark records. They were the
data_points.extend(pyspark_parser.make_graph(id='7511'))  # process IDs when the code was profiled, present in every JSON records
data_points.extend(pyspark_parser.make_graph(id='7512'))  # outputted by PySpark profiler as value in 'pid' fields

fig = Figure(data=data_points)
plot(fig, filename='fatso-pyspark.html')