from plotly.offline import plot
from parsers import AppParser, SparkLogParser
import plotly.graph_objs as go
from plotly.graph_objs import Figure
from helper import get_max_y

#################################################################################################

# Concurrency profile for Straggler:
logfile = './data/ProfileStraggler/JobStraggler.log.gz'
log_parser = SparkLogParser(logfile)
log_parser.extract_stage_markers()
data = []
interval_markers = log_parser.extract_stage_markers()
data.append(interval_markers)

active_tasks = log_parser.get_active_tasks_plot()
data.append(active_tasks)
job_intervals = log_parser.job_intervals
max_y = get_max_y(data)
layout = log_parser.extract_job_markers(max_y)
trace0 = go.Scatter()
fig = Figure(data=data, layout=layout)
plot(fig, filename='conc-straggler.html')


#################################################################################################

# Concurrency profile for whole application:
log_path = '/data/application_1597675138635_0007/'
app_parser = AppParser(log_path)
data = []
active_tasks = app_parser.get_active_tasks_plot()
data.append(active_tasks)
job_intervals = app_parser.get_job_intervals
max_y = get_max_y(data)
layout = app_parser.extract_job_markers(max_y)
trace0 = go.Scatter()
fig = Figure(data=data, layout=layout)
plot(fig, filename='conc-app.html')
