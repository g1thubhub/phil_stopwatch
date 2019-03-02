# Phil's Stopwatch for profiling Spark

A tech blog can be found hereXXX

The Scala sources for the riddles in can be found in this companion project

Compositionality

Most classes are compositional:

An **AppParser** object is basically a list of **SparkLogParser** objects which in turn might wrap a **ProfileParser** object


Many function available in *AppParser* internally delegate to the implementation in *SparkLogParser* and collect/merge/combine/... the individual results into a single return value for the collection of *SparkLogParser*s

Examples can be found in the script in the *analytics* directory





## Input
The input to all script functionality are records from a Profiler or Spark logs. The design is compositional so log records and profiler records can be in separate files or in one file

## Profilers
file containing JSON format
Uber's JVM profiler or Phil's PySpark profiler

### Uber's JVM profiler

or used xxx

In a distributed environment like AWS, Spark executors need to access this JAR file. 

* 
Upload JAR to S3, accessible to cluster
spark-submit --deploy-mode cluster --class uk.co.streamhub.executors.ads.vr.AdsExecutor --driver-memory 30g --conf spark.jars=s3://streamhub-releases/batch/qa/phil/jvm-profiler-0.0.9.jar --conf spark.driver.extraJavaOptions=-javaagent:jvm-profiler-0.0.9.jar=sampleInterval=2000,metricInterval=1000 --conf spark.executor.extraJavaOptions=-javaagent:jvm-profiler-0.0.9.jar=sampleInterval=2000,metricInterval=1000 --num-executors 39 --executor-cores 3 s3://streamhub-releases/batch/staging/phil/batch_6795.jar --run-type fixed --run-mode manual --year 2019 --week 2 -p 400 --partitions-write 10 -r daily --metric ad:uniqueViewers






## Analyzing and Visualizing records

## Uber's JVM profiler
Uber's [JVM profiler](https://github.com/uber-common/jvm-profiler) has several advantages so this project assumes that it will be used as the JVM profiler (the [ProfileParser](https://github.com/g1thubhub/phil_stopwatch/blob/e83645f44e7fecf43331e0b1dcf9920a6deb027c/parsers.py#L59) code could be easily modified to use outputs from other profilers though). The profiler JAR can be built with the following commands:
```terminal
$ git clone https://github.com/uber-common/jvm-profiler.git
$ cd jvm-profiler/
$ mvn clean package
[...]
Replacing /users/phil/jvm-profiler/target/jvm-profiler-1.0.0.jar with /users/phil/jvm-profiler/target/jvm-profiler-1.0.0-shaded.jar

$ ls
-rw-r--r--   1 a  staff  7097056  9 Feb 10:07 jvm-profiler-1.0.0.jar
drwxr-xr-x   3 a  staff       96  9 Feb 10:07 maven-archiver
drwxr-xr-x   3 a  staff       96  9 Feb 10:07 maven-status
-rw-r--r--   1 a  staff    92420  9 Feb 10:07 original-jvm-profiler-1.0.0.jar
jvm-profiler-1.0.0.jar

```
... or you can use the JAR that I built from [here](https://github.com/g1thubhub/philstopwatch/blob/master/src/main/resources/jvm-profiler-1.0.0.jar)


## Profiling a single JVM / Spark in local mode
The following command was used to generate the output uploaded [here](https://github.com/g1thubhub/phil_stopwatch/tree/master/analytics/data/ProfileStraggler) from the [JobStraggler](https://github.com/g1thubhub/philstopwatch/blob/master/src/main/scala/profile/sparkjobs/JobStraggler.scala) class:
```terminal
~/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
--conf spark.driver.extraJavaOptions=-javaagent:/users/phil/jvm-profiler/target/jvm-profiler-1.0.0.jar=sampleInterval=1000,metricInterval=100,reporter=com.uber.profiling.reporters.FileOutputReporter,outputDir=./ProfileStraggler \
--class profile.sparkjobs.JobStraggler \
target/scala-2.11/philstopwatch-assembly-0.1.jar > JobStraggler.log 
```

## Profiling executor JVMs
When Spark is launched in distributed mode, most of the actual work is done by Spark executors that run on remote cluster nodes. In order to profile their VMs, the following `conf` setting would need to be added to the launch command above:
```terminal
--conf spark.executor.extraJavaOptions=[...]
```
This is redundant in "local" mode though since the driver and executor run in the same JVM -- the profile records are already created by including *--conf spark.driver.extraJavaOptions*. An actual example of the command used for profiling a legit distributed Spark job is included further below.

## Profiling PySpark
The output of executing the PySpark edition of [Straggler](https://github.com/g1thubhub/phil_stopwatch/blob/master/spark_jobs/job_straggler.py) is included in [this](https://github.com/g1thubhub/phil_stopwatch/tree/master/analytics/data/profile_straggler) repo folder, here is the command that was used:
```terminal
~/spark-2.4.0-bin-hadoop2.7/bin/spark-submit \
--conf spark.driver.extraJavaOptions=-javaagent:/users/phil/jvm-profiler/target/jvm-profiler-1.0.0.jar=sampleInterval=1000,metricInterval=100,reporter=com.uber.profiling.reporters.FileOutputReporter,outputDir=./profile_straggler \
--conf spark.python.profile=true \
./spark_jobs/job_straggler.py  cpumemstack /users/phil/phil_stopwatch/analytics/data/profile_straggler > Straggler_PySpark.log
```
As in the run of the "Scala" edition above, the second line activates the JVM profiling, only the output directory name has changed (*profile_straggler* instead of *ProfileStraggler*). This line is optional here since a PySpark app is launched but it might make sense to include this JVM profiling it as a majority of the work is performed outside of Python. The third line and the two input arguments to the script in the last line (`cpumemstack` and `/users/phil/phil_stopwatch/analytics/data/profile_straggler`) are required for the actual PySpark profiler: The config parameter (*--conf spark.python.profile=true*) tells Spark that a custom profiler will be used, the first script argument *cpumemstack* specifies the profiler class (a profiler that tracks CPU, memory and the stack) and the second argument specifies the directory to where the profiler records will be saved. 

Three PySpark profiler classes are included in [pyspark_profilers.py](https://github.com/g1thubhub/phil_stopwatch/blob/master/pyspark_profilers.py): [StackProfiler](https://github.com/g1thubhub/phil_stopwatch/blob/e83645f44e7fecf43331e0b1dcf9920a6deb027c/pyspark_profilers.py#L119) can be used to catch stack traces in order to create flame graphs, [CpuMemProfiler](https://github.com/g1thubhub/phil_stopwatch/blob/e83645f44e7fecf43331e0b1dcf9920a6deb027c/pyspark_profilers.py#L27) captures CPU and memory usage, and [CpuMemStackProfiler](https://github.com/g1thubhub/phil_stopwatch/blob/e83645f44e7fecf43331e0b1dcf9920a6deb027c/pyspark_profilers.py#L206) is a combination of these two. In order to use them, the `profiler_cls` field needs to be set with the name of the profiler as in this [example](https://github.com/g1thubhub/phil_stopwatch/blob/e83645f44e7fecf43331e0b1dcf9920a6deb027c/spark_jobs/job_straggler.py#L29) when constructing a *SparkContext* so there are three possible settings:
* profiler_cls=StackProfiler
* profiler_cls=CpuMemProfiler
* profiler_cls=CpuMemStackProfiler

I added a dictionary `profiler_map` in [helper.py](https://github.com/g1thubhub/phil_stopwatch/blob/master/helper.py) that links these class names with abbreviations that can be used as the actual script arguments when launching a PySpark app:
* `spark-submit [...] your_script.py stack ` sets `profiler_cls=StackProfiler`
* `spark-submit [...] your_script.py cpumem ` sets `profiler_cls=CpuMemProfiler`
* `spark-submit [...] your_script.py cpumemstack ` sets `profiler_cls=CpuMemStackProfiler`

The PySpark profiler code in [pyspark_profilers.py](https://github.com/g1thubhub/phil_stopwatch/blob/master/pyspark_profilers.py) needs a few auxiliary methods defined in helper.py. In case of a distributed application, Spark executors running on cluster nodes 
also need to access these two files, the easiest (but probably not most elegant) way of doing this is via SparkContext's `adFile` method, this solution is used [here](https://github.com/g1thubhub/phil_stopwatch/blob/e83645f44e7fecf43331e0b1dcf9920a6deb027c/spark_jobs/job_straggler.py#L31).

If such a distributed application is launched, it might not be possible to create output files in this fashion on storage systems like HDFS or S3 so profiler records might need to be written to the standard output instead. This can easily be accomplished by using the appropriate function in the appliction source code: The line ... 
```python
session.sparkContext.dump_profiles(dump_path)
``` 
... would change into ...
```python
session.sparkContext.show_profiles()
``` 
An example occurrence is [here](https://github.com/g1thubhub/phil_stopwatch/blob/e83645f44e7fecf43331e0b1dcf9920a6deb027c/spark_jobs/job_straggler.py#L46) 


### Distributed Profiling
The last sentences already described some of the changes required for distributed PySpark profiling. For distributed JVM profiling, the worker nodes need to access the `jvm-profiler-1.0.0.jar` file so this JAR should be uploaded to the storage layer, in the case of S3 the copy command would be
```terminal
aws s3 cp ./jvm-profiler-1.0.0.jar s3://your/bucket/jvm-profiler-1.0.0.jar
```

In a cloud environment, it is likely that the `FileOutputReporter` used above (set via *reporter=com.uber.profiling.reporters.FileOutputReporter*) will not work since its source code does not seem to include functionality for interacting with storage layers like HDFS or S3. In these cases, the profiler output records can be written to standard out along with other messages. This happens by default when no explicit `reporter=` flag is set as in the following command:
```terminal
spark-submit --deploy-mode cluster \ 
--class your.Class \
--conf spark.jars=s3://your/bucket/jvm-profiler-1.0.0.jar \
--conf spark.driver.extraJavaOptions=-javaagent:jvm-profiler-1.0.0.jar=sampleInterval=2000,metricInterval=1000 \
--conf spark.executor.extraJavaOptions=-javaagent:jvm-profiler-1.0.0.jar=sampleInterval=2000,metricInterval=1000 \
s3://path/to/your/project.jar 
```
The actual source code of the Spark application is located in the class `Class` inside package `your` and all source code is packaged inside the JAR file `project.jar`. The third line specifies the location of the profiler JAR that all Spark executors and the driver need to be able to access in case they are profiled. They are indeed, the fourth and fifth line activate *CpuAndMemory* profiling and *Stacktrace* sampling.



## Analyzing and Visualizing

The code in this repo operates on two types of input, on the output records of a profiler described in the previous paragraphs or on Spark log files. Since the design is compositional, the records can be mixed in one or split across several files.

### Analyzing and visualizing a local job / 4 riddles
The source code of the four riddles inside the [spark_jobs](https://github.com/g1thubhub/phil_stopwatch/tree/master/spark_jobs) folder was executed in "local mode". Several scripts that produce visualizations and reports of the output of these riddles are included in the [analytics](https://github.com/g1thubhub/phil_stopwatch/tree/master/analytics) folder. 

An example of a script that extracts "everything"   -- metric graphs, Spark -- and visualizes that is pasted below:

```python
from plotly.offline import plot
from plotly.graph_objs import Figure
from parsers import ProfileParser, SparkLogParser
from helper import get_max_y

# Create a ProfileParser object to extract metrics graph:
profile_file = './data/ProfileStraggler/CpuAndMemory.json.gz'  # Output from JVM profiler
profile_parser = ProfileParser(profile_file, normalize=True)  # normalize various metrics
data_points = profile_parser.make_graph()  # create graph lines of various metrics

# Create a SparkLogParser object to extract task/stage/job boundaries:
log_file = './data/ProfileStraggler/JobStraggler.log.gz'  # standard Spark log
log_parser = SparkLogParser(log_file)

max = get_max_y(data_points)  # maximum y-value used to scale task lines extracted below:s
task_data = log_parser.graph_tasks(max)  # create graph lines of all Spark tasks
data_points.extend(task_data)

stage_interval_markers = log_parser.extract_stage_markers()  # extract stage boundaries and will show on x-axis
data_points.append(stage_interval_markers)
layout = log_parser.extract_job_markers(max)  # extracts job boundaries and will show as vertical dotted lines

# Plot the actual gaph and save it in 'everything.html'
fig = Figure(data=data_points, layout=layout)
plot(fig, filename='everything.html')
```

### Analyzing and visualizing a distributed application
When launching a distributed application, Spark executors run on multiple nodes in a cluster and produce several log files, one per executor/container. In a cloud environment like AWS, these log files will be organized in the following structure:
```terminal
s3://aws-logs/elasticmapreduce/clusterid-1/containers/application_1_0001/container_1_001/
                                                                                         stderr.gz
                                                                                         stdout.gz
s3://aws-logs/elasticmapreduce/clusterid-1/containers/application_1_0001/container_1_002/
                                                                                         stderr.gz
                                                                                         stdout.gz
[...]
s3://aws-logs/elasticmapreduce/clusterid-1/containers/application_1_0001/container_1_N/
                                                                                         stderr.gz
                                                                                         stdout.gz

[...]

s3://aws-logs/elasticmapreduce/clusterid-M/containers/application_K_0001/container_K_L/
                                                                                         stderr.gz
                                                                                         stdout.gz
```
An EMR cluster like `clusterid-1` might run several Spark applications consecutively, each one as its own step. Each application launched a number of containers, `application_1_0001` for example launched executors `container_1_001`, `container_1_002`, ..., `container_1_N`. Each of these container created a standard error and a standard out file on S3. In order to analyze a particular application like `application_1_0001` above, all of its associated log files like *.../application_1_0001/container_1_001/stderr.gz* and *.../application_1_0001/container_1_001/stdout.gz* are needed. The easiest way is to collect all files under the _application_ folder using a command like ...
```terminal
aws s3 cp --recursive s3://aws-logs/elasticmapreduce/clusterid-1/containers/application_1_0001/ ./application_1_0001/
```
... and then to create an [AppParser](https://github.com/g1thubhub/phil_stopwatch/blob/d2e1697c380e7e5a3f16d064131f66da2f0d98ac/parsers.py#L729) object object like 
```terminal
from parsers import AppParser
app_path = './application_1_0001/'  # path to the application directory downloaded from s3 above
app_parser = AppParser(app_path)
```
This object creates a number of [SparkLogParser](https://github.com/g1thubhub/phil_stopwatch/blob/d2e1697c380e7e5a3f16d064131f66da2f0d98ac/parsers.py#L239) objects internally (one for each container) and automatically identifies the "master" log file created by the Spark driver (likely located under `application_1_0001/container_1_001/`). Several useful functions can now be called on `app_parser`, example scripts are located in the [analytics](https://github.com/g1thubhub/phil_stopwatch/tree/master/analytics) folder and more detailled explanations can be found in the [readme](https://github.com/g1thubhub/phil_stopwatch) file.

### Finding Error messages and top log chunks
The script (extract_heckler)[https://github.com/g1thubhub/phil_stopwatch/blob/master/analytics/extract_heckler.py] shows how to extract top log chunks and the most recent error messages from an individual log file or from a collection of log files that form an application:

In the case of "top log chunks", the function `SparkLogParser.get_top_log_chunks` applies a pattern matching and collapsing algorithm across multiple consecutive log lines and creates a ranked list of these top log chunks as output. 

The function `AppParser.extract_errors()` tries to deduplicate potential exceptions and error messages and will print them out in reverse chronological order. An exception or error message might occur several times during a run with slight variations (e.g., different timestamps or code line numbers) but the last occurrence is the most important one for debugging purposes since it might be the direct cause for the failure.

### Creating Flame Graphs
The profilers described above might produce stacktraces -- *Stacktrace.json* files in the case of the JVM profiler and *s_N_stack.json* files in the case of the PySpark profilers. These outputs can be folded and transformed into flame graphs with the help of my [fold_stacks.py](https://github.com/g1thubhub/phil_stopwatch/blob/master/fold_stacks.py) script and this external script: [flamegraph.pl](https://github.com/brendangregg/FlameGraph/blob/master/flamegraph.pl)

For JVM stack traces like *./analytics/data/ProfileFatso/StacktraceFatso.json.gz*, use
```terminal
Phils-MacBook-Pro:analytics a$ python3 fold_stacks.py ./analytics/data/ProfileFatso/StacktraceFatso.json.gz  > Fatso.folded
Phils-MacBook-Pro:analytics a$ perl flamegraph.pl Fatso.folded > FatsoFlame.svg
```
The final output file *FatsoFlame.svg* can be opened in a browser. The procedure is identical for PySpark stacktraces like *./analytics/data/profile_fatso/s_8_stack.json*:
```terminal
Phils-MacBook-Pro:analytics a$ python3 fold_stacks.py ./analytics/data/profile_fatso/s_8_stack.json  > FatsoPyspark.folded
Phils-MacBook-Pro:analytics a$ perl flamegraph.pl  FatsoPyspark.folded  > FatsoPySparkFlame.svg
```
A combined JVM/PySpark 

Normalized units


spaCy
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"



