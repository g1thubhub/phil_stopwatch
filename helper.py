import plotly.graph_objs as go
import time
from numbers import Number

metric_definitions = """[
    ["JVMProfiler", {
        "epochMillis": ["ms", "", true],
        "ScavengeCollTime": ["ms", "ms_to_minutes", false],
        "MarkSweepCollTime": ["ms", "ms_to_minutes", false],
        "MarkSweepCollCount": ["int", "", false],
        "ScavengeCollCount": ["int", "", false],
        "systemCpuLoad": ["float", "", true],
        "processCpuLoad": ["float", "", true],
        "nonHeapMemoryTotalUsed": ["byte", "byte_to_mb", true],
        "nonHeapMemoryCommitted": ["byte", "byte_to_mb", true],
        "heapMemoryTotalUsed": ["byte", "byte_to_mb", true],
        "heapMemoryCommitted": ["byte", "byte_to_mb", true]
    }
     ],
    ["PySparkPhilProfiler", {
        "epochMillis": ["ms", "", true],
        "pmem_rss": ["byte", "byte_to_mb", true],
        "pmem_vms": ["byte", "byte_to_mb", true],
        "cpu_percent": ["float", "", true]
    }
     ]
]"""

# For PySpark jobs:

class Object:
    def __init__(self, string):
        self.string = string

# For log & profile parsing

time_patterns = {
    r'^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)': '%Y-%m-%d %H:%M:%S',  # 2019-01-05 19:38:41
    r'^(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d)': '%Y/%m/%d %H:%M:%S',  # 2019/01/05 19:38:41
    r'^(\d\d/\d\d/\d\d \d\d:\d\d:\d\d)': '%y/%m/%d %H:%M:%S',   # 19/01/05 19:38:41
    r'^(\d\d-\d\d-\d\d \d\d:\d\d:\d\d)': '%y-%m-%d %H:%M:%S',   # 19-01-05 19:38:41
}



# 2019-01-05 10:21:57 INFO  SparkContext:54 - Submitted application: Profile (1)
r_app_start = r'.* submitted application: (.*)'

# 2019-01-05 10:30:21 INFO  TaskSetManager:54 - Starting task 0.0 in stage 1.0 (TID 3, localhost, executor driver, partition 0, PROCESS_LOCAL, 8249 bytes)
# Ri
r_task_start = r'.* (?:starting|running) task (\d+\.\d+) in stage (\d+\.\d+).*'

# 2019-01-05 10:30:21 INFO  Executor:54 - Finished task 2.0 in stage 1.0 (TID 5). 1547 bytes result sent to driver
r_task_end = r'.* finished task (\d+\.\d+) in stage (\d+\.\d+).*'

# 2019-01-05 10:22:02 INFO  DAGScheduler:54 - Got job 0 (foreach at ProfileStragglerSM.scala:37) with 3 output partitions
r_job_start = r'.* got job (\d+) '
# 2019-01-05 10:30:19 INFO  DAGScheduler:54 - Job 0 finished: foreach at ProfileStragglerSM.scala:37, took 497.646962 s
r_job_end = r'.* job (\d+) finished'


r_spark_log = r'.* (error|info|warn)(.*)'

r_container_id = r'.*container_\d+_\d+_\d+_0*(\d+).*'




# Unit conversion methods
def identity(num):
    return num

def ms_to_seconds(ms):
    return ms / 1000

def ms_to_minutes(ms):
    return ms / 60000

def ns_to_minute(ns):
    return ns / 60000000000

def byte_to_mb(bytes):
    return bytes / (1024*1024)

def to_epochms(spark_timestamp):
    return int(time.mktime(spark_timestamp.timetuple()) * 1000 + spark_timestamp.microsecond/1000)


conversion_map = {
    "": identity,
    "ns_to_minute": ns_to_minute,
    "ms_to_minutes": ms_to_minutes,
    "byte_to_mb": byte_to_mb
}


def extract_nested_keys(structure, key_acc):
    if isinstance(structure, dict):
        for k, v in structure.items():
            if isinstance(v, list):
                extract_nested_keys(v, key_acc)
            elif isinstance(v, dict):
                extract_nested_keys(v, key_acc)
            key_acc.add(k)
    elif isinstance(structure, list):
        for i in structure:
            if isinstance(i, list):
                extract_nested_keys(i, key_acc)
            elif isinstance(i, dict):
                extract_nested_keys(i, key_acc)
    return key_acc


def graph_tasks(starttimes, endtimes, maximum):
    data = []
    tasks_x = []
    tasks_y = []
    texts = []
    multiplier = 1
    distance = maximum / len(starttimes)   # y-distance between stage lines
    for task_stage_id in starttimes:
        text = 'Task ' + '@'.join((str(task_stage_id[0]), str(task_stage_id[1])))
        starttime = starttimes[task_stage_id]
        tasks_x.append(starttime)
        tasks_y.append(task_stage_id[0] + task_stage_id[1] + distance*multiplier)
        texts.append(text + ' Start')
        scatter = go.Scatter(
            name=text,
            x=[starttime, endtimes[task_stage_id]],
            y=[task_stage_id[0] + task_stage_id[1] + distance*multiplier, task_stage_id[0] + task_stage_id[1] + distance*multiplier],
            mode='lines+markers',
            hoverinfo='none',
            line=dict(color='darkblue', width=5),
            opacity=0.5
        )
        data.append(scatter)

        endtime = endtimes[task_stage_id]
        tasks_x.append(endtime)
        tasks_y.append(task_stage_id[0] + task_stage_id[1] + distance*multiplier)
        texts.append(text + ' End')

        multiplier += 1

    return data, tasks_x, tasks_y, texts

def cover(prefix, arr):
    arr1 = arr.copy()
    n = 0
    while arr1[:len(prefix)] == prefix:
        arr1 = arr1[len(prefix):]
        n += 1
    return n, arr1

def cost(t):
    s, r, n = t
    c = len(s) + len(r) * n
    return c, -r, -s

def findcovers(stack, maxfraglen):
    covers = []
    for fraglen in range(1, maxfraglen + 1):
        fragment = stack[:fraglen]
        n, _ = cover(fragment, stack)
        if n >= 2:
            covers.append((fraglen, n))
    return covers


def collapse(stack):
    results = []
    lastsingle = False

    while len(stack) > 0:
        covers = findcovers(stack, maxfraglen=10)

        if len(covers) == 0:
            element = stack[:1]
            if lastsingle:
                lastfrag, _ = results[-1]
                results[-1] = (lastfrag + element, 1)
            else:
                results.append((element, 1))
                lastsingle = True
            stack = stack[1:]
        else:
            R, N = max(covers, key=lambda t: (t[0] * t[1], -t[0]))
            results.append((stack[:R], N))
            stack = stack[R*N:]
            lastsingle = False

    return results


def get_max_y(data):
    max_y = -1
    for datapoint in data:
        numbers = filter(lambda x: isinstance(x, Number), datapoint.y)
        current_max_y = max(numbers)
        if current_max_y > max_y:
            max_y = current_max_y
    return max_y


def fat_function_inner(i):
    new_list = list()
    for j in range(0, i):
        new_list.append(j)
    return new_list

def secondsSleep(i):
    time.sleep(1)
    return i
