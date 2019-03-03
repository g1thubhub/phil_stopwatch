from plotly.offline import plot
from typing import List
from parsers import AppParser, SparkLogParser
from helper import get_max_y
from plotly.graph_objs import Figure, Scatter

# Path to the log files of an application, structure:
application_path = './data/application_1551152464841_0001'

#################################################################################################

# Active tasks for the whole application, parses master log file internally
app_parser = AppParser(application_path)
data: List[Scatter] = list()

active_tasks = app_parser.get_active_tasks_plot()
job_intervals = app_parser.get_job_intervals
max_y = get_max_y(data)

data.append(active_tasks)
layout = app_parser.extract_job_markers(max_y)
fig = Figure(data=data, layout=layout)
plot(fig, filename='bigjob-concurrency.html')


#################################################################################################

# memory profile for all executors, parses all log files except for the "master log file" internally
app_parser = AppParser(application_path)
data_points: List[Scatter] = list()

executor_logs: List[SparkLogParser] = app_parser.get_executor_logparsers()
for parser in executor_logs:
    # print(parser.get_available_metrics())  # ['epochMillis', 'ScavengeCollTime', 'MarkSweepCollTime', 'MarkSweepCollCount', 'ScavengeCollCount', 'systemCpuLoad', 'processCpuLoad', 'nonHeapMemoryTotalUsed', 'nonHeapMemoryCommitted', 'heapMemoryTotalUsed', 'heapMemoryCommitted']
    relevant_metric: List[Scatter] = parser.get_metrics(['heapMemoryTotalUsed'])
    data_points.extend(relevant_metric)

max_y = get_max_y(data_points)  # static method, maximum y value needed for cosmetic reasons, scaling tasks
stage_interval_markers = app_parser.extract_stage_markers()

data_points.append(stage_interval_markers)
layout = app_parser.extract_job_markers(max_y)
fig = Figure(data=data_points, layout=layout)
plot(fig, filename='bigjob-memory.html')


#################################################################################################

# tasks, stages, job annotations for the whole application, parses "master log file" internally
app_parser = AppParser(application_path)
data_points: List[Scatter] = list()

max_y = 1000  # heuristic since we're not plotting metrics here, for cosmetic purposes

task_markers = app_parser.graph_tasks(max_y)
stage_interval_markers = app_parser.extract_stage_markers()
layout = app_parser.extract_job_markers(max_y)

data_points.append(stage_interval_markers)
data_points.extend(task_markers)

fig = Figure(data=data_points, layout=layout)
plot(fig, filename='bigjob-tasks.html')