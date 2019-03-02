from plotly.offline import plot
from plotly.graph_objs import Figure
from parsers import ProfileParser, SparkLogParser
from helper import get_max_y


profile_file = './data/ProfileStraggler/CpuAndMemory.json.gz'  # Output from JVM profiler
profile_parser = ProfileParser(profile_file, normalize=True)
# data_points = profile_parser.ignore_metrics(['ScavengeCollCount'])
data_points = profile_parser.get_metrics(['systemCpuLoad', 'processCpuLoad'])

log_file = './data/ProfileStraggler/JobStraggler.log.gz'  # standard Spark log
log_parser = SparkLogParser(log_file)

max = get_max_y(data_points)
task_data = log_parser.graph_tasks(max)

data_points.extend(task_data)
stage_interval_markers = log_parser.extract_stage_markers()
data_points.append(stage_interval_markers)

layout = log_parser.extract_job_markers(max)
fig = Figure(data=data_points, layout=layout)
plot(fig, filename='straggler.html')
