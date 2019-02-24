---
layout: default
---

Made in London 

# 4 BigData Riddles: The Straggler, The Slacker, The Fatso & The Heckler
<p></p>

This article discusses four bottlenecks for BigData applications and introduces a number of tools, some new, for identifying and solving them. The 


The riddles occur particular framework Apache Spark Scala and Python 
Spark applications.
<p></p>
These applications have something in common: They require around 10 minutes _wall clock time_ when a "local version" of them is run on a commodity notebook. The easiest  -- using more processors/machines or more powerful machines -- would not significantly reduce the execution time. 

<p></p>
But there are also differences between the applications: Each one has a different kind of bottleneck that is responsible for slowness and each of these bottlenecks can be identified with a different tool. Some of these tools -- among them flamegraphs, resource profiles and log files -- and methods for combining them are not widely known or do not exist and will be presented in the second half of this article along with complete source code and additional explanations for all  


I will talk about the riddles as a kind of black box first, the code that will be shown in the second part of this article.
Means of iddentifying the bottleneck is different, custom scripts or programs were usedd
I will describe the riddles and their "solutions" first, then I will introduce the methods for solbing them in more detail.  



means of identification for these botlenecks is different and don't exist in this form

Using more machines or more powerful machines for the riddles will not help


<br>
<br>

## The Fatso

The _Fatso_ in a might be inherent to BigData. 


### Spark JVM Profile

The fatso is almost soften occurs in the BigData world. A symptom is noise -- 

The job qualifies as a fatso 


Using my project introduced below in this way using just a few lines of code:

```python
profile_file = './data/ProfileFatso/CpuAndMemoryFatso.json.gz'  # Output from JVM profiler
profile_parser = ProfileParser(profile_file, normalize=True)
data_points = profile_parser.make_graph()


logfile = './data/ProfileFatso/Fatso.log'
log_parser = SparkLogParser(logfile)
stage_markers = log_parser.extract_stage_markers()
data_points.append(stage_markers)

max_y = get_max_y(data_points)
layout = log_parser.extract_job_markers(max_y)
fig = Figure(data=data_points, layout=layout)
plot(fig, filename='fatso.html')
```

This code (full version hereXXX) combines two different information sources (the output of a profiler and normal Spark logs) into a single visualization. This interactive graph can be analyzed in its full glory [hereXXX](http://127.0.0.1:4000/fatso.html). A smaller snapshot is displayed below:

<p></p>
![Fatso profile](images/fatso.png)
<p></p>
Spark's execution model consists of different units of different "granularity levels" and some of these are displayed above: Boundaries of Spark jobs are represented as vertical dashed lines, start and end points of Spark stages are displayed as transparent blue dots on the x-axis which also show the full stage IDs. This scheduling information does not add a lot of insight here since _Fatso_ consists of only one Spark job which in turn consists of just a single Spark stage (comprised of three tasks) but, as shown below, knowing such time points can be very helpful when analyzing more complex applications.

For all graphs in this article, the x-axis shows the application run time as UNIX Epoch time (milliseconds passed since 1 January 1970). The y-axis represents different normalized units for different metrics: For lines representing memory metrics such as _total heap memory used_ ("heapMemoryTotalUsed", turquoise line above), it represents gigabytes; for time measurements like _MarkSweep GC collection time_ ("MarkSweepCollTime", orange line above), data points on the y-axis represent milliseconds. More details can be found in thisXXXX data struture which can be changed or augmented with new metrics from  different profilers.

<br>
One available metric, _ScavengeCollCount_, is absent from the snapshot above but present in the original [graph](http://127.0.0.1:4000/fatso.html). It signifies a minor garbage collection event and almost linearly increases up to 20000 during _Fatso_'s execution. In other words, the application ran for almost 10 minutes (from XX to YY ) and more than 20000 minor Garbage Collection events and almost 70 major GC events ("MarkSweepCollCount", green line) occurred. 
<br>
When the application was launched, no configuration parameters were manually set so the default Spark settings applied. This means that the maximum memory available to the program was 1GB. Having a closer look at the two heap memory metrics _heapMemoryCommitted_ and _heapMemoryTotalUsed_ reveals that both lines approach this 1GB ceiling near the end of the application.
<br>
The intermediate conclusion that can be drawn from the discussion so far is that the application is very memory hungry and a lot of GC activity is going on, but the exact reason for this is still unclear. A second tool can help now:


<br>

### Spark JVM FlameGraph

```terminal
Phils-MacBook-Pro:analytics a$ python3 fold_stacks.py ./analytics/data/ProfileFatso/StacktraceFatso.json.gz  > Fatso.folded
Phils-MacBook-Pro:analytics a$ perl flamegraph.pl Fatso.folded > FatsoFlame.svg
```
Opening _FatsoFlame.svg_ in a browser:

{% include FatsoFlame.svg %}
A rule of thumb for the interpretation of flame graphs is: The more spiky the shape, the better. We see many plateaus above with native Spark/Java functions like _sun.misc.unsafe.park_ sitting on top (first plateau) or low-level functions from packages like _io.netty_ occurring near the top, this is a 3rd party library that Spark depends on for network communication / IO. The only functions in the picture that are defined by me are located in the center plateau, searching for the package name _profile.sparkjob_. On top of them are Array and String functions. Indeed, they create many Strings objects in an efficient way we have identified the two _Fatso_ method that need to be optimized.


<br>

### Python/PySpark Profiles
What about Spark applications written in Python? I created [several profilers](https://github.com/g1thubhub/phil_stopwatch/blob/master/pyspark_profilers.py) that try to provide some of the functionality of Uber's JVM profiler. Because of the architecture of PySpark, it might be beneficial to generate both Python and JVM profiles in order to get a good grasp of the overall resource usage. This can be accomplished for the Python edition of _Fatso_ by using the following launch command (abbreviated, full version hereXXX)
```terminal
~/spark-2.4.0-bin-hadoop2.7/bin/spark-submit  \
--conf spark.python.profile=true  \
--conf spark.driver.extraJavaOptions=-javaagent:/.../=sampleInterval=1000,metricInterval=100,reporter=...outputDir=... \
 ./spark_jobs/job_fatso.py  cpumemstack  /Users/phil/phil_stopwatch/analytics/data/profile_fatso  >  Fatso_PySpark.dat 
```
As above, the *--conf* parameter in the third line is responsible for attaching the JVM profiler. The --conf parameter in the second line as well as the two script arguments in the last line are new and required for PySpark profiling: The _cpumemstack_ argument will choose a PySpark profiler that captures both CPU/memory usage as well as stack traces. By providing a second script argument in the form of a directory path, it is ensured that the profile records are written into separate output files instead of just printing all of them to the standard output.

<br>
Similar to its Scala cousin, the PySpark edition of _Fatso_  completes in around 10 minutes on my MacBook and and creates several JSON files in the specified output directory. The JVM profile could be visualized idenpendently of the Python profile but it might be more insightful to create a single combined graph from them. This can be accomplished easily and is shown in the second half of thisXXX script. The full combined graph is located [here](http://127.0.0.1:4000/fatso-pyspark.html)

![Fatso PySpark profile](images/fatso-pyspark.png)
<p></p>
The clever reader will already have a hunch about the high memory consumption and who is responsible for it: The garbage collection activity of the JVM that is again represented by _MarkSweepCollCount_ and _ScavengeCollCount_ is much lower here compared to the "pure" Spark run described in the previous paragraphs (20000 events above versus less than 20 GC events now). The two inefficient _fatso_ functions are now implemented in Python and therefore not managed by the JVM leading to far fewer JVM memory usage and GC events. A PySpark flamegraph should confirm our hunch:
<p></p>

```terminal
Phils-MacBook-Pro:analytics a$ python3 fold_stacks.py ./analytics/data/profile_fatso/s_8_stack.json  > FatsoPyspark.folded
Phils-MacBook-Pro:analytics a$ perl flamegraph.pl  FatsoPyspark.folded  > FatsoPySparkFlame.svg
```
Opening _FatsoPySparkFlame.svg_ in a browser displays ...
{% include FatsoPySparkFlame.svg %}
<p></p>
And indeed, two _fatso_ methods sit ontop the stack for almost 90% of all measurements burning most CPU cycles.
It would be easy to create a combined JVM/Python flamegraph by concatenating the respective stacktrace files. This would be of limited use here though since the JVM flamegraph will likely consist entirely of native Java/Spark functions over which a Python coder has no control. One scenario I can think of where this merging of JVM with PySpark stacktraces might be especilly useful is when Java code or libraries are registered and called from PySpark/Python code which is getting easier and easier in newer versions of Spark. In the discussion of _Slacker_ later on, I will present a combined stack trace of Python and Scala code.   

<br>
<br>
## The Straggler
The _Straggler_ is deceiving: It appears as if all computational resources are fully utilized most the time and only closer analysis can reveal that this might be the case for only a small subset of the system or for a limited period of time. The following graph created from [this](https://github.com/g1thubhub/phil_stopwatch/blob/master/analytics/plot_straggler.py) script combines two CPU metrics with information about task and stage boundaries extracted from the standard logging output of a typical straggler run; the full size graph can be found [here](http://127.0.0.1:4000/straggler.html)
<p></p>
![Straggler profile](images/straggler.png)

The associated application consisted of one Spark job which is represented as vertical dashed lines at the left and right. This single job was comprised of a single stage, shown as transparent blue dots on the x axis that coincide with the job start and end points. But there were three tasks within that stage so we can see three horizontal task lines. The naming schema of this execution hierarchy is not arbitrary:
- The stage name in the graph is **0.0@0** because a stage with the id **0.0** which belonged to a job with id **0** is referred to. The first part of stage or task names is a floating point number, this reflects the apparent naming convention in Spark logs that new attempts of failed task or stages are baptized with an incremented fraction part.
- The task names are **0.0@0.0@0**, **1.0@0.0@0**, and **2.0@0.0@0** because three tasks were launched that were all members of stage **0.0@0** that in turn belonged to job **0**

The three tasks have the same start time which almost coincides with the application's invocation but very different end times: Tasks *1.0@0.0@0* and *2.0@0.0@0* finish within the first fifth of the application's lifetime whereas task *0.0@0.0@0* stays alive for almost the entire application since its start and end points are located at the left and right borders of this graph.
The orange and light blue lines visualize two CPU metrics (_system cpu load_ and _process cpu load_) whose fluctuations correspond with the task activity: We can observe that the CPU load drops right after tasks *1.0@0.0@0* and *2.0@0.0@0* end. It stays at around 20% for 4/5 of the time, when only straggler task *0.0@0.0@0* is running.  BLIPXXXX

### Concurrency Profiles
When an application consists of more than just one stage with three tasks like _Straggler_, it might be more illuminating to calculate and represent the total number of tasks that were running at any point during the application's lifetime. The "concurrency profile" of a BigData workload might look more like 
{% include conc-profile.html %}

The source code that is the basis for the graph can be found in [this](https://github.com/g1thubhub/phil_stopwatch/blob/master/analytics/plot_application.py) script. The big difference to the _Straggler_ toy example before is that in real-life applications, many different log files are compiled (one for each container/Spark executor) and there is only one "master" log file which contains necessary information about scheduling and task boundaries which are needed to make concurrency profiles. The script uses an `AppParser` [class](https://github.com/g1thubhub/phil_stopwatch/blob/28c174f25a1cfab5003998656e4c0ae96fb95384/parsers.py#L687) that does this automatically by creating a list of `LogParser` objects (one for each container) and then parsing them to determine the master log.
<br>
We can attempt a back-of-the-envelope calculation to increase the efficiency of the application from just looking at this concurrency profile: In case around 80 physical CPU cores were used (given multiple peaks of ~80 active tasks), we can hypothesize that the application was "overallocated" by at least 20 CPU cores or 4 to 7 Spark executors or one to three nodes as Spark executors are often configured to use 3 to 5 physical CPU cores. Reducing the machines reserved for this application should not increase its execution time but it will give more resources to other users in a shared cluster setting or save some $$ in a cloud environment.

<br>
### A Fratso

What about the specs of the actual compute nodes used? The memory profile for a similar app created via [this](https://github.com/g1thubhub/phil_stopwatch/blob/28c174f25a1cfab5003998656e4c0ae96fb95384/analytics/plot_application.py#L27) code segment is chaotic yet illuminating since more than 50 Spark executors/containers were launched by application and each one left its mark in the graph in the form of a memory metric line (original located [here](http://127.0.0.1:4000/bigjob-memory.html))
![BigJob Mem](images/bigjob-memory.png)

The peak heap memory used is a little more than 10GB, one executor crosses this 10k line twice (top right) while most other executors use at most 8-9 GB or less. Removing the memory usage from the picture and displaying scheduling information like task boundaries instead results in the following graph

![BigJob Tasks](images/bigjob-tasks.png) 

The application launches several small Spark jobs initially as indicated by the occurrence of multiple dashed lines near the left border. However, more than 90% of the total execution time is consumed by a single big job which has the ID _8_. A closer look at the blue dots on the x-axis that represent boundaries of Spark stages reveals that there are two longer stages within job _8_. During the first stage, there are four task waves without stragglers -- concurrent tasks that together look like solid blue rectangles when visualized this way. The second stage of job _8_ does have a straggler task as there is one horizontal blue task line that is much longer active than its "neighbour" tasks. Looking back at the memory graph of this application, it is likely that this straggler task is also responsible for the heap memory peak of >10GB that we discovered. We might have identified a "fratso" here (a straggling fatso) and this task/stage should definitely be analyzed in more detail when improving the associated application.
<br>
<br>
The script that generated all three previous plots can be found [here](https://github.com/g1thubhub/phil_stopwatch/blob/master/analytics/plot_application.py).

<br>
<br>
## The Heckler: CoreNLP & spaCy

Applying NLP or machine learning methods often involves the use of third party libraries which in turn create quite memory-intensive objects. There are several different ways of constructing such heavy classifiers in Spark so that each task can access them, the first version of the _Heckler_ code that is the topic of this section will do that in the worst possible way. I am not aware of a metric currently exposed by Spark that could directly show such inefficiencies, something similar to a measure of network transfer from master to executors would be required for one case below. The identification of this bottleneck must therefore happen indirectly by applying some more sophisticated string matching and collapsing logic to Spark's standard logs:

```python
log_file = './data/ProfileHeckler1/JobHeckler1.log.gz'
log_parser = SparkLogParser(log_file)
collapsed_ranked_log: List[Tuple[int, List[str]]] = log_parser.get_top_log_chunks()
for line in collapsed_ranked_log[:5]:  # print 5 most frequently occurring log chunks
    print(line)
```
Executing the [script](https://github.com/g1thubhub/phil_stopwatch/blob/master/analytics/extract_heckler.py) containing this code segment produces the following output:
```terminal
Phils-MacBook-Pro:analytics a$ python3 extract_heckler.py
 
^^ Identified time format for log file: %Y-%m-%d %H:%M:%S
 
(329, ['StanfordCoreNLP:88 - Adding annotator tokenize', 'StanfordCoreNLP:88 - Adding annotator ssplit', 
'StanfordCoreNLP:88 - Adding annotator pos', 'StanfordCoreNLP:88 - Adding annotator lemma', 
'StanfordCoreNLP:88 - Adding annotator parse'])
(257, ['StanfordCoreNLP:88 - Adding annotator tokenize', 'StanfordCoreNLP:88 - Adding annotator ssplit', 
'StanfordCoreNLP:88 - Adding annotator pos', 'StanfordCoreNLP:88 - Adding annotator lemma', 
'StanfordCoreNLP:88 - Adding annotator parse'])
(223, ['StanfordCoreNLP:88 - Adding annotator tokenize', 'StanfordCoreNLP:88 - Adding annotator ssplit', 
'StanfordCoreNLP:88 - Adding annotator pos', 'StanfordCoreNLP:88 - Adding annotator lemma', 
'StanfordCoreNLP:88 - Adding annotator parse'])
(221, ['StanfordCoreNLP:88 - Adding annotator tokenize', 'StanfordCoreNLP:88 - Adding annotator ssplit', 
'StanfordCoreNLP:88 - Adding annotator pos', 'StanfordCoreNLP:88 - Adding annotator lemma', 
'StanfordCoreNLP:88 - Adding annotator parse'])
(197, ['StanfordCoreNLP:88 - Adding annotator tokenize', 'StanfordCoreNLP:88 - Adding annotator ssplit', 
'StanfordCoreNLP:88 - Adding annotator pos', 'StanfordCoreNLP:88 - Adding annotator lemma', 
'StanfordCoreNLP:88 - Adding annotator parse'])
```
These are the 5 most frequent log chunks found in the logging file, each one is a pair `[int, List[str]]`. The left integer signifies the total number of times the right list of log segments occurred in the file; each individual member in the list occurred in a separate log line in the file. Hence the return value of the method `get_top_log_chunks` that created the output above has the type annotation `List[Tuple[int, List[str]]]`, it extracts a ranked list of contiguous log segments. 

The top record can be interpreted the following way: The four strings
```terminal
StanfordCoreNLP:88 - Adding annotator tokenize
StanfordCoreNLP:88 - Adding annotator ssplit 
StanfordCoreNLP:88 - Adding annotator pos
StanfordCoreNLP:88 - Adding annotator lemma
StanfordCoreNLP:88 - Adding annotator parse
```
occurred as infixes in this order 329 times in total in the log file. They were likely part of longer log lines as normalization and collapsing logic was applied by the extraction algorithm, an example occurrence of the first part of the chunk (`StanfordCoreNLP:88 - Adding annotator tokenize`) would be 
```terminal
2019-02-16 08:44:30 INFO StanfordCoreNLP:88 - Adding annotator tokenize
```

What does this tell us? The associated Spark app seems to have performed some NLP tagging since log4j messages from the Stanford [CoreNLP](https://stanfordnlp.github.io/CoreNLP/) project can be found as part of the Spark logs. Initializing a `StanfordCoreNLP` object ...
```scala
  val props = new Properties()
  props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse")

  val pipeline = new StanfordCoreNLP(props)
  val annotation = new Annotation("This is an example sentence")

  pipeline.annotate(annotation)
  val parseTree = annotation.get(classOf[SentencesAnnotation]).get(0).get(classOf[TreeAnnotation])
  println(parseTree.toString) // prints (ROOT (NP (NN Example) (NN sentence)))
```
... yields the following log4j output ...
```terminal
0 [main] INFO edu.stanford.nlp.pipeline.StanfordCoreNLP  - Adding annotator tokenize
9 [main] INFO edu.stanford.nlp.pipeline.StanfordCoreNLP  - Adding annotator ssplit
13 [main] INFO edu.stanford.nlp.pipeline.StanfordCoreNLP  - Adding annotator pos
847 [main] INFO edu.stanford.nlp.tagger.maxent.MaxentTagger  - Loading POS tagger from [...] done [0.8 sec].
848 [main] INFO edu.stanford.nlp.pipeline.StanfordCoreNLP  - Adding annotator lemma
849 [main] INFO edu.stanford.nlp.pipeline.StanfordCoreNLP  - Adding annotator parse
1257 [main] INFO edu.stanford.nlp.parser.common.ParserGrammar  - Loading parser from serialized [...] ... done [0.4 sec].
``` 
... which tells us that five annotators (*tokenize, ssplit, pos, lemma, parse*) are created and wrapped inside a single `StanfordCoreNLP` object. Concerning the use of CoreNLP with Spark,
the number of cores/tasks used in _Heckler_ is three (as it is in all other riddles) which means that we should find at most three occurrences of these annotator messages in the corresponding Spark log file. But we already saw more than 1000 occurrences when only the top 5 log chunks were investigated above. Having a closer look at the _Heckler_ source [code](https://github.com/g1thubhub/philstopwatch/blob/164e6ab0ac55ccab356c286ba3912c334bea7b27/src/main/scala/profile/sparkjobs/JobHeckler.scala#L47) resolves this contradiction, the implementation is bad since one classifier object is recreated for every input sentence that will be syntactially annotated -- there are 60000 input sentences in total so an `StanfordCoreNLP` object will be constructed a staggering 60000 times. Due to the distributed/concurrent nature of _Heckler_, we don't always see the annotator messages in the order `tokenize - ssplit - pos - lemma - parse` because log messages of task (1) might interweave with log messages of task (2) and (3) in the actual log file which is also the reason for the slightly reordered log chunks in the top 5 list.

<br>
Improving this inefficient implementation is not too difficult: Creating the classifier inside a `mapPartitions` instead of a `map` function as done [here](https://github.com/g1thubhub/philstopwatch/blob/164e6ab0ac55ccab356c286ba3912c334bea7b27/src/main/scala/profile/sparkjobs/JobHeckler.scala#L59) will only create three *StanfordCoreNLP* objects overall. However, this is not the minimum, I will now set the record for creating the smallest number of tagger objects with the minimum amount of network transfer: Since `StanfordCoreNLP` is not serializable per se, it needs to be wrapped inside a class that is in order to prevent a _java.io.NotSerializableException_ when broadcasting it later:
```scala
class DistribbutedStanfordCoreNLP extends Serializable {
  val props = new Properties()
  props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse")
  lazy val pipeline = new StanfordCoreNLP(props)
}
[...]
val pipelineWrapper = new DistribbutedStanfordCoreNLP()
val pipelineBroadcast: Broadcast[DistribbutedStanfordCoreNLP] = session.sparkContext.broadcast(pipelineWrapper)
[...]
val parsedStrings3 = stringsDS.map(string => {
   val annotation = new Annotation(string)
   pipelineBroadcast.value.pipeline.annotate(annotation)
   val parseTree = annotation.get(classOf[SentencesAnnotation]).get(0).get(classOf[TreeAnnotation])
   parseTree.toString
})
```
The proof lies in the logs:
```terminal
19/02/23 18:48:45 INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
19/02/23 18:48:45 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
19/02/23 18:48:45 INFO Executor: Running task 2.0 in stage 0.0 (TID 2)
19/02/23 18:48:46 INFO StanfordCoreNLP: Adding annotator tokenize
19/02/23 18:48:46 INFO StanfordCoreNLP: Adding annotator ssplit
19/02/23 18:48:46 INFO StanfordCoreNLP: Adding annotator pos
19/02/23 18:48:46 INFO MaxentTagger: Loading POS tagger from [...] ... done [0.6 sec].
19/02/23 18:48:46 INFO StanfordCoreNLP: Adding annotator lemma
19/02/23 18:48:46 INFO StanfordCoreNLP: Adding annotator parse
19/02/23 18:48:47 INFO ParserGrammar: Loading parser from serialized file edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz ... done [0.4 sec].
19/02/23 18:59:07 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 1590 bytes result sent to driver
19/02/23 18:59:07 INFO Executor: Finished task 2.0 in stage 0.0 (TID 2). 1590 bytes result sent to driver
19/02/23 18:59:07 INFO Executor: Finished task 1.0 in stage 0.0 (TID 1). 1590 bytes result sent to driver
```
I'm not sure about the multi-threading capabilities of `StanfordCoreNLP` so it might turn out that the second "per partition" solution is superior performance-wise to the [third](https://github.com/g1thubhub/philstopwatch/blob/164e6ab0ac55ccab356c286ba3912c334bea7b27/src/main/scala/profile/sparkjobs/JobHeckler.scala#L73). In any case, we reduced the number of tagging objects created from 60000 to three or one, not bad.

### spaCy on PySpark

The PySpark version of _Heckler_ will use [spaCy](https://spacy.io/) (written in Cython/Python) as NLP library instead of *CoreNLP*. From the perspective of a JVM aficionado, packaging in Python itself is odd and spaCy doesn't seem to be very chatty. Therefore I created an initialization [function](https://github.com/g1thubhub/phil_stopwatch/blob/a1a088facf08eafae806e3958d26cf948d1538f1/spark_jobs/job_heckler.py#L15) that should print more log messages and address potential issues when running spaCy in a distributed enviroment as model files need to be present on every Spark executor.

As expected, the "bad" [implementation](https://github.com/g1thubhub/phil_stopwatch/blob/a1a088facf08eafae806e3958d26cf948d1538f1/spark_jobs/job_heckler.py#L48) of _Heckler_ recreates one spaCy NLP model per input sentence as proven by this logging excerpt:
```terminal
[Stage 0:>                                                          3 / 3]
^^ Using spaCy 2.0.18
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Using spaCy 2.0.18
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Using spaCy 2.0.18
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Created model en_core_web_sm
^^ Created model en_core_web_sm
^^ Created model en_core_web_sm
^^ Using spaCy 2.0.18
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Using spaCy 2.0.18
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Using spaCy 2.0.18
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Created model en_core_web_sm
^^ Created model en_core_web_sm
^^ Created model en_core_web_sm
[...]
```

Inspired by the Scala edition of _Heckler_, the "per partition" PySpark [solution](https://github.com/g1thubhub/phil_stopwatch/blob/a1a088facf08eafae806e3958d26cf948d1538f1/spark_jobs/job_heckler.py#L36) only initialize three spacy NLP objects during the application's lifetime, the complete log file of that run is short:
```terminal
[Stage 0:>                                                          (0 + 3) / 3]
^^ Using spaCy 2.0.18
^^ Using spaCy 2.0.18
^^ Using spaCy 2.0.18
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Model found at /usr/local/lib/python3.6/site-packages/spacy/data/en_core_web_sm
^^ Created model en_core_web_sm
^^ Created model en_core_web_sm
^^ Created model en_core_web_sm
1500
```

<br>
### Finding failure messages
The functionality introduced in the previous paragraphs can be modified a bit to make the investigation of failed applications much easier: The reason for a crash is often not immediately apparent and requires sifting through log files. Resource-intensive applications will create many log files (one per container/Spark executor) so search functionality along with deduplication and pattern matching logic should come in handy here: The function `extract_errors` from the [AppParser](https://github.com/g1thubhub/phil_stopwatch/blob/c55ec3e5e821eed3e47ec86a8b2ecf03c8090c59/parsers.py#L783) class tries to deduplicate potential exceptions and error messages and print them out in reverse chronological order. An exception or error message might occur several times during a run with slight variations (e.g., different timestamps or code line numbers) but the last occurrence is the most important one for debugging purposes since it might be the direct cause for the failure.

```python
app_path = './data/application_1549675138635_0005'
app_parser = AppParser(app_path)
app_errors: Deque[Tuple[str, List[str]]] = app_parser.extract_errors()

for error in app_errors:
    print(error)
```


```terminal
^^ Identified app path with log files
^^ Identified time format for log file: %y/%m/%d %H:%M:%S
^^ Warning: Not all tasks completed successfully: {(16.0, 9.0, 8), (16.1, 9.0, 8), (164.0, 9.0, 8), ...}
^^ Extracting task intervals
^^ Extracting stage intervals
^^ Extracting job intervals

Error messages found, most recent ones first:
```

('/Users/phil/data/application_1549_0005/container_1549_0001_02_000002/stderr.gz', ['18/02/01 21:49:35 ERROR ApplicationMaster: User class threw exception: org.apache.spark.SparkException: Job aborted.', 'org.apache.spark.SparkException: Job aborted.', 'at org.apache.spark.sql.execution.datasources.FileFormatWriter$$anonfun$write$1.apply$mcV$sp(FileFormatWriter.scala:213)', 'at org.apache.spark.sql.execution.datasources.FileFormatWriter$$anonfun$write$1.apply(FileFormatWriter.scala:166)', 'at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:65)', 'at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:166)', 'at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:145)', 'at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:58)', 'at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:56)', 'at org.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(commands.scala:74)', 'at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:117)', 'at org.apache.spark.sql.execution.SparkPlan$$anonfun$executeQuery$1.apply(SparkPlan.scala:138)', 'at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)', 'at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:135)', 'at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:116)', 'at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:92)', 'at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:92)', 'at org.apache.spark.sql.execution.datasources.DataSource.writeInFileFormat(DataSource.scala:435)', 'at org.apache.spark.sql.execution.datasources.DataSource.write(DataSource.scala:471)', 'at org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:50)', 'at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:58)', 'at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:56)', 'at org.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(commands.scala:74)', 'at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:117)', 'at org.apache.spark.sql.execution.SparkPlan$$anonfun$executeQuery$1.apply(SparkPlan.scala:138)', 'at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)', 'at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:135)', 'at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:116)', 'at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:92)', 'at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:92)', 'at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:609)', 'at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:233)', 'at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:217)', 'at org.apache.spark.sql.DataFrameWriter.parquet(DataFrameWriter.scala:508)', 
[...]
'... 48 more'])

<br>
('/Users/phil/data/application_1549_0005/container_1549_0001_02_000124/stderr.gz', ['18/02/01 21:49:34 ERROR CoarseGrainedExecutorBackend: RECEIVED SIGNAL TERM'])

<br>
('/Users/phil/data/application_1549_0005/container_1549_0001_02_000002/stderr.gz', ['18/02/01 21:49:34 WARN YarnAllocator: Container marked as failed: container_1549731000_0001_02_000124 on host: ip-172-18-39-28.ec2.internal. Exit status: -100. Diagnostics: Container released on a *lost* node'])

<br>
('/Users/phil/data/application_1549_0005/container_1549_0001_02_000002/stderr.gz', ['18/02/01 21:49:34 WARN YarnSchedulerBackend$YarnSchedulerEndpoint: Container marked as failed: container_1549731000_0001_02_000124 on host: ip-172-18-39-28.ec2.internal. Exit status: -100. Diagnostics: Container released on a *lost* node'])

<br>
('/Users/phil/data/application_1549_0005/container_1549_0001_02_000002/stderr.gz', ['18/02/01 21:49:34 ERROR TaskSetManager: Task 30 in stage 9.0 failed 4 times; aborting job'])

<br>
('/Users/phil/data/application_1549_0005/container_1549_0001_02_000002/stderr.gz', ['18/02/01 21:49:34 ERROR YarnClusterScheduler: Lost executor 62 on ip-172-18-39-28.ec2.internal: Container marked as failed: container_1549731000_0001_02_000124 on host: ip-172-18-39-28.ec2.internal. Exit status: -100. Diagnostics: Container released on a *lost* node'])

<br>
('/Users/phil/data/application_1549_0005/container_1549_0001_02_000002/stderr.gz', ['18/02/01 21:49:34 WARN TaskSetManager: Lost task 4.3 in stage 9.0 (TID 610, ip-172-18-39-28.ec2.internal, executor 62): ExecutorLostFailure (executor 62 exited caused by one of the running tasks) Reason: Container marked as failed: container_1549731000_0001_02_000124 on host: ip-172-18-39-28.ec2.internal. Exit status: -100. Diagnostics: Container released on a *lost* node'])

<br>
('/Users/phil/data/application_1549_0005/container_1549_0001_02_000002/stderr.gz', ['18/02/01 21:49:34 WARN ExecutorAllocationManager: No stages are running, but numRunningTasks != 0'])

[...]
<br>
<br>

Each error record printed out in this fashion consists of two elements: The first one is a path to the source log to the source log in which the second element, the actual error chunk, was found.




stack trance -- a multiline error message that is normalized and copied into a single listhere

reverse chronological order, pattern matching


Some are benign


concurrency, triple
concurrent nature, they might interweave 

pattern matching, contiguous subseqences
handy

Flooding, down from 316 error lines down to just 16, sort from 
internally collapse (repeated segment) 

possibility of finding errors
reverse chronologcial order

<br>
<br>
## The Slacker


```python
combined_file = './data/profile_slacker/CombinedCpuAndMemory.json.gz'  # Output from JVM & PySpark profilers

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
```

The output is ...
```terminal
JVM max values:
{'ScavengeCollTime': 0.0013, 'MarkSweepCollTime': 0.00255, 'MarkSweepCollCount': 3.0, 'ScavengeCollCount': 10.0,
'systemCpuLoad': 0.6419753086419753, 'processCpuLoad': 0.6189945167759597, 'nonHeapMemoryTotalUsed': 89.07969665527344,
'nonHeapMemoryCommitted': 90.3125, 'heapMemoryTotalUsed': 336.95963287353516, 'heapMemoryCommitted': 452.0}

PySpark max values:
{'pmem_rss': 78.50390625, 'pmem_vms': 4448.35546875, 'cpu_percent': 0.4}
```
These are low values given a baseline overhead of running Spark and comparing the profiles for _Fatso_ above -- for example, only 13 GC events happened and the peak CPU load for the entire run was less than 65%. Visualizing all CPU data points shows that these maxima occured at the beginning / end of the application when there is always lots of initialization and cleanup work regardless of the logic being executed (bigger version [hereXXX](http://127.0.0.1:4000/slacker-cpu.html)): 
![Slacker PySpark profile](images/cpu-usage.png)
<br>
So the system is almost idle for the majority of the time. The slacker in this pure form is a rare sight; when processing real-life workloads, slacking most likely occurs in certain stages that interact with an external system like querying a database for records that should be joined with Datasets/RDDs later on or that materialize output records to a storage layer like HDFS and use not enough write partitions. A combined flame graph of JVM and Python stack traces will reveal the slacking part, original [hereXXX](http://127.0.0.1:4000/CombinedStack.svg)
{% include CombinedStack.svg %}

In the first plateau which is also the longest, two custom Python functions sit at the top. After inspecting their implementation [here](https://github.com/g1thubhub/phil_stopwatch/blob/edef3b88425717ede93c33683bd0c59f85ba40c6/spark_jobs/job_slacker.py#L15) and [there](https://github.com/g1thubhub/phil_stopwatch/blob/edef3b88425717ede93c33683bd0c59f85ba40c6/helper.py#L211), the low system utilization should not be surprising anymore: The second function from the top,`job_slacker.py:slacking`, is basically a simple loop that calls a function `helper.py:secondsSleep` from an external _helper_ package many times. This function has a sample presence of almost 20% (seen in the [originalXXX](http://127.0.0.1:4000/CombinedStack.svg)) and, since it sits atop the plateau, is executed by the CPU most of the time. As its function name suggests, it causes the program to sleep for one second. So _Slacker_ is esentially a 10 minute long system sleep.
In real-world BigData application that have slacking phases, we can expect the top of some plateaus to be occupied by "write" functions like `FileFormatWriter$$anonfun$write$1.apply(FileFormatWriter.scala` or by functions related to DB queries.
<br>
<br>

## Code & Goodies

### Riddle source code
The 

tried to make things fairly compositional / composable -- for example, the output of a profiler can occur in one or across several files, either alone or in combination with log messages (of different formats!) s or the output of another profiler, the code will take care of that
Unit conversion in order to display various metrics conveniently in the same graph. You can check the map XXX and also add your own metrics.


### Riddle source code

The Scala source code for the local riddles are here

The Python versions are here

The Python / PysSpark editions are here

### Uber's JVM profiler

Existing JVM applications can be profiled without modifying any source code. I built it in the following way:


```terminal
git clone https://github.com/uber-common/jvm-profiler.git
cd jvm-profiler/
mvn clean package
[...]
Replacing /Users/a/jvm-profiler/target/jvm-profiler-1.0.0.jar with /Users/a/jvm-profiler/target/jvm-profiler-1.0.0-shaded.jar
```
The jvm-profiler-1.0.0.jar is a self-contained "fat jar" and can be used for profiling.

S3.

In a cloud environment it is likely to not use the FileOutputReporter since there is no code that can interact with for HDFS or S3. Printing to standard out, the parser will take care of this as shown above. 


### PySpark
```shell
export PYSPARK_PYTHON=python3
pip3 install -e .

```
### JVM & PySpark Profilers


java.lang.management package  (Java 1.5)


javax.management.MX


Three different PySpark profilers in this [file](https://github.com/g1thubhub/phil_stopwatch/blob/master/pyspark_profilers.py): A CPU/Memory profiler, a Stacktrace profiler and a combination of these two.

If the JVM profiler is used with the options shown above, three different kinds of records are generated which, in case the _FileOutputReporter_ flag is used, are written to three JSON files, _ProcessInfo.json_, _CpuAndMemory.json_ and _Stacktrace.json_. Similarly, my PySpark profiler will create two different output records that, with the appropriate flag set, are written to at least two JSON files with the pattern _s_*_stack.json_ or  _s_*_cpumem.json_.
In case of Java/Scala, three In case 





At least two additional JSON files created by my PySpark profilers, 
At least five JSON files, 3 are created by the JVM profiler and the rest are created by 
CpuAndMemory.json


cloud/hadoop environment part of "standard out" since XXX does not implement an HDFS FileWriter, I might start working on tjat soon



