# Readme
*hoho*


Compositionality

Most classes are compositional:

An **AppParser** object is basically a list of **SparkLogParser** objects which in turn might wrap a **ProfileParser** object


Many function available in *AppParser* internally delegate to the implementation in *SparkLogParser* and collect/merge/combine/... the individual results into a single return value for the collection of *SparkLogParser*s

Examples can be found in the script in the *analytics* directory



## Readme 2

See this blog posts for some examples


## Input
The input to all script functionality are records from a Profiler or Spark logs. The design is compositional so log records and profiler records can be in separate files or in one file

## Profiler records
file containing JSON format
Uber's JVM profiler or Phil's PySpark profiler

### JVM profiler


```terminal
$ git clone https://github.com/uber-common/jvm-profiler.git
$ cd jvm-profiler/
$ mvn clean package
[...]
Replacing /Users/a/jvm-profiler/target/jvm-profiler-1.0.0.jar with /Users/a/jvm-profiler/target/jvm-profiler-1.0.0-shaded.jar

$ ls
-rw-r--r--   1 a  staff  7097056  9 Feb 10:07 jvm-profiler-1.0.0.jar
drwxr-xr-x   3 a  staff       96  9 Feb 10:07 maven-archiver
drwxr-xr-x   3 a  staff       96  9 Feb 10:07 maven-status
-rw-r--r--   1 a  staff    92420  9 Feb 10:07 original-jvm-profiler-1.0.0.jar
jvm-profiler-1.0.0.jar

```




* 
Upload JAR to S3, accessible to cluster
spark-submit --deploy-mode cluster --class uk.co.streamhub.executors.ads.vr.AdsExecutor --driver-memory 30g --conf spark.jars=s3://streamhub-releases/batch/qa/phil/jvm-profiler-0.0.9.jar --conf spark.driver.extraJavaOptions=-javaagent:jvm-profiler-0.0.9.jar=sampleInterval=2000,metricInterval=1000 --conf spark.executor.extraJavaOptions=-javaagent:jvm-profiler-0.0.9.jar=sampleInterval=2000,metricInterval=1000 --num-executors 39 --executor-cores 3 s3://streamhub-releases/batch/staging/phil/batch_6795.jar --run-type fixed --run-mode manual --year 2019 --week 2 -p 400 --partitions-write 10 -r daily --metric ad:uniqueViewers


### PySpark Profiler

In combination 


set the `profiler_cls` field with the name of the profiler when constructing a *SparkContext* so
the three prossible values when using my profilers are:
* profiler_cls=StackProfiler
* profiler_cls=CpuMemProfiler
* profiler_cls=CpuMemStackProfiler


I added a dictionary `profiler_map` in (helpers.py)[sdas] that links these class names to shorter names that can be used as the actual script arguments when launchen a PySpark app:
* `spark-submit [...] your_script.py stack ` sets `profiler_cls=StackProfiler`
* `spark-submit [...] your_script.py cpumem ` sets `profiler_cls=CpuMemProfiler`
* `spark-submit [...] your_script.py cpumemstack ` sets `profiler_cls=CpuMemStackProfiler`





## Analyzing and Visualizing records

The code in this repo operates on two types of input, the output records of a profiler described described in the previous two paragraphs or the Spark log files. Since the design is compositional, the records can be mixed and split: All records can be in one file (in case the profiler wrote to standard out) and there can be many individual log files (as when running in a distributed environment).





An Example is here

The profiler code is contained in the file pyspark_profilers.py which needs a few methods defined in helper.py. In case of a distributed application, Spark executors running on cluster nodes 
also need to access these two files, the easiest (but probably not most elegant) way of doing this is via SparkContext's `adFile` method, an example can be found hereXXX


### Profiling a riddle


session.sparkContext.show_profiles()

### Cloud environment




Normalized units


spaCy
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"




export SBT_OPTS="-Xmx1536M -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=2G -Xss2M"

sbt clean assembly
[info] Packaging /Users/a/IdeaProjects/philstopwatch/target/scala-2.11/philstopwatch-assembly-0.1.jar






### Log files
either a single file that standard out

or path to an application which contains subdirectories and files in this format, typically run in a cloud environment

aws s3 cp --recursive XXXX ./

application ID 