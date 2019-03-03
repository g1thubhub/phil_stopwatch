from plotly.offline import plot
from plotly.graph_objs import Figure, Scatter
from typing import List
from parsers import ProfileParser, SparkLogParser
from helper import get_max_y


# Create a ProfileParser object to extract metrics graph:
profile_file = './data/ProfileStraggler/CpuAndMemory.json.gz'  # Output from JVM profiler
profile_parser = ProfileParser(profile_file, normalize=True)  # normalize various metrics
data_points: List[Scatter] = profile_parser.make_graph()  # create graph lines of various metrics

# Create a SparkLogParser object to extract task/stage/job boundaries:
log_file = './data/ProfileStraggler/JobStraggler.log.gz'  # standard Spark log
log_parser = SparkLogParser(log_file)

max: int = get_max_y(data_points)  # maximum y-value used to scale task lines extracted below:s
task_data: List[Scatter] = log_parser.graph_tasks(max)  # create graph lines of all Spark tasks
data_points.extend(task_data)

stage_interval_markers: Scatter = log_parser.extract_stage_markers()  # extract stage boundaries and will show on x-axis
data_points.append(stage_interval_markers)
layout = log_parser.extract_job_markers(max)  # extracts job boundaries and will show as vertical dotted lines

# Plot the actual gaph and save it in 'everything.html'
fig = Figure(data=data_points, layout=layout)
plot(fig, filename='everything.html')
