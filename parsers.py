import json
import re
from datetime import datetime
import os
from functools import reduce
import operator
import gzip
import hashlib
import glob
from typing import Tuple, List, Deque, Dict
from collections import deque, defaultdict
import plotly.graph_objs as go
from plotly.graph_objs import Scatter
from helper import r_container_id, metric_definitions, time_patterns, r_spark_log, r_app_start, r_task_start, r_task_end, \
    r_job_start, r_job_end, extract_nested_keys, conversion_map, to_epochms, ms_to_seconds, get_max_y


class StackParser:
    @staticmethod
    def open_stream(file):
        if file.endswith('gz'):
            return gzip.open(file, 'rt')
        else:
            return open(file, 'r')

    @staticmethod
    def convert_file(file, id='', merge=True):
        StackParser.convert_files([file], id, merge)

    @staticmethod
    def convert_files(file_list, id='', merge=True):
        stacks = defaultdict(int)

        for file in file_list:
            stream = StackParser.open_stream(file)
            for line in stream:
                line = line.strip()
                if not (line.startswith('{') and line.endswith('}') and '"stacktrace":' in line):
                    continue
                stack_record = json.loads(line)
                if 'count' not in stack_record:
                    continue
                count = stack_record['count']
                stacktrace = ';'.join(list(reversed(stack_record['stacktrace'])))

                if 'i_d' in stack_record and id != '' and stack_record['i_d'] != id:  # skip for PySpark stack traces
                    continue

                if merge:
                    stacks[stacktrace] += count
                else:
                    print(' '.join((stacktrace, str(count))))

            stream.close()
        for (stack, count) in stacks.items():
            print(' '.join((stack, str(count))))


class ProfileParser:
    def __init__(self, filename, normalize=True):
        self.filename = filename
        self.normalize = normalize  # normalize units
        self.resource_format = ''  # currently JVMProfiler or PySparkPhilProfiler
        self.data_points = list()  # list of dictionaries
        self.metric_conversions = list()  # json file
        self.relevant_metrics = dict()  # {'epochMillis': (<function identity at 0x119437730>, True),
        self.profile_match_keys = set()

        metric_map = json.loads(metric_definitions)
        for profiler in metric_map:
            profiler_name = profiler[0]
            metric_map = profiler[1]
            self.metric_conversions.append((profiler_name, dict(map(lambda kv: (kv[0], (conversion_map[kv[1][1]], kv[1][2])), metric_map.items()))))

    def parse_profiles(self, id=''):
        if self.resource_format == '':  # First valid JSON profile record in file sets for whole file
            self.deduce_profiler()
        self.data_points.clear()

        stream = self.open_stream()
        for line in stream:
            currentline = line.strip().replace('ConsoleOutputReporter - CpuAndMemory: ', '')  # In case JVM Profiler wrote to STDOUT
            # Profile record can be part of log file so checks here:
            if not (currentline.startswith('{') and currentline.endswith('}')):
                continue
            resource_usage = json.loads(currentline)
            record_keys = extract_nested_keys(resource_usage, set())
            if len(self.profile_match_keys - record_keys) != 0:  # in case a file contains records from two different profilers
                continue

            if self.resource_format == 'PySparkPhilProfiler' and id != '':
                if str(resource_usage['pid']) != id:
                    continue
            metrics = dict()

            for relevant_metric in self.relevant_metrics.items():
                if relevant_metric[1][1] is True:
                    metric_name = relevant_metric[0]
                    value = resource_usage[metric_name]
                    if self.normalize:  # Convert metrics so they can be visualized conveniently together
                        value = self.relevant_metrics[metric_name][0](value)
                    metrics[metric_name] = value

            # Custom parsing logic of 4 GC metrics for JVM profiler
            if self.resource_format == 'JVMProfiler':
                memory_pools = resource_usage['memoryPools']
                codecache = memory_pools[0]
                assert codecache['name'] == 'Code Cache'
                metaspace = memory_pools[1]
                assert metaspace['name'] == 'Metaspace'
                metaspace = memory_pools[2]
                assert metaspace['name'] == 'Compressed Class Space'
                metaspace = memory_pools[3]
                assert metaspace['name'] == 'PS Eden Space'
                # # ,"gc":[{"collectionTime":97,"name":"PS Scavenge","collectionCount":13},{"collectionTime":166,"name":"PS MarkSweep","collectionCount":3}]}
                gc = resource_usage['gc']
                scavenge = gc[0]
                marksweep = gc[1]
                assert scavenge['name'] == "PS Scavenge"
                assert marksweep['name'] == "PS MarkSweep"

                scavenge_count = scavenge['collectionCount']
                scavenge_time = scavenge['collectionTime']
                marksweep_count = marksweep['collectionCount']
                marksweep_time = marksweep['collectionTime']

                if self.normalize:
                    scavenge_count = self.relevant_metrics['ScavengeCollCount'][0](scavenge_count)
                    scavenge_time = self.relevant_metrics['ScavengeCollTime'][0](scavenge_time)
                    marksweep_count = self.relevant_metrics['MarkSweepCollCount'][0](marksweep_count)
                    marksweep_time = self.relevant_metrics['MarkSweepCollTime'][0](marksweep_time)

                metrics['ScavengeCollCount'] = scavenge_count
                metrics['ScavengeCollTime'] = scavenge_time
                metrics['MarkSweepCollCount'] = marksweep_count
                metrics['MarkSweepCollTime'] = marksweep_time

            self.data_points.append(metrics)
        self.data_points.sort(key=lambda entry: entry['epochMillis'])  # sorting based on timestamp
        stream.close()
        print('## Parsed file, number of data points: ' + str(len(self.data_points)))

    def open_stream(self):
        if self.filename.endswith('gz'):
            return gzip.open(self.filename, 'rt')
        else:
            return open(self.filename, 'r')

    def get_available_metrics(self, id='') -> List[str]:
        if len(self.data_points) == 0:
            self.parse_profiles(id)
        return list(self.relevant_metrics.keys())

    def get_maxima(self) -> Dict[str, float]:
        maxima = {}
        all_metrics: List[Scatter] = self.ignore_metrics(list())
        for metric in all_metrics:
            max_value = get_max_y([metric])
            maxima[metric.name] = float(max_value)
        return maxima

    def deduce_profiler(self):
        # Determining format of profiler used
        stream = self.open_stream()
        for line in stream:
            currentline = line.strip()  #
            currentline = currentline.replace('ConsoleOutputReporter - CpuAndMemory: ', '')  # In case JVM Profiler wrote to STDOUT
            if currentline.startswith('{') and currentline.endswith('}'):  # could be part of a log file
                profile_record = json.loads(currentline)
                record_keys = extract_nested_keys(profile_record, set())
                for profile in self.metric_conversions:
                    profile_match_keys = set([item[0] for item in profile[1].items() if item[1][1] is True])
                    delta = profile_match_keys - record_keys
                    if len(delta) == 0:
                        self.resource_format = profile[0]
                        self.relevant_metrics = profile[1]
                        self.profile_match_keys = profile_match_keys
            if self.resource_format != '':
                break
        if self.resource_format != '':
            print('## Identified Profile for ' + self.filename + ' as ' + self.resource_format)
        else:
            raise ValueError('Unknown profile format for file ' + self.filename)
        stream.close()

    def manually_set_profiler(self, profile):
        # Setting format of profiler used
        normalized_profile = profile.lower()
        for profile in self.metric_conversions:
            if normalized_profile in profile[0].lower():
                self.resource_format = profile[0]
                self.relevant_metrics = profile[1]
                profile_match_keys = set([item[0] for item in profile[1].items() if item[1][1] is True])
                self.profile_match_keys = profile_match_keys
                print('## Set Profile for ' + self.filename + ' to ' + self.resource_format)

    def make_graph(self, id='') -> List[Scatter]:
        if len(self.data_points) == 0:
            self.parse_profiles(id)
        if len(self.data_points) == 0:
            print('## No data points')
            return None

        display_keys = list(self.relevant_metrics.keys())
        display_keys.remove('epochMillis')
        data_points = list()
        for display_key in display_keys:
            display_key_name = display_key
            if id != '':
                display_key_name += '_' + id
            data_points.append(go.Scatter(x=list(map(lambda x: x['epochMillis'], self.data_points)), y=list(map(lambda x: x[display_key], self.data_points)),
                                              mode='lines+markers', name=display_key_name))
        return data_points

    def get_metrics(self, names=list(), id='') -> List[Scatter]:
        if len(names) == 0:
            return self.make_graph()
        else:
            all_metrics = self.make_graph(id)
            relevant_metrics = list(filter(lambda x: any([ele in x['name'] for ele in names]), all_metrics))
            return relevant_metrics

    def ignore_metrics(self, names=list()) -> List[Scatter]:
        if len(names) == 0:
            return self.make_graph()
        else:
            all_metrics = self.make_graph()
            relevant_metrics = list(filter(lambda x: any([ele not in x['name'] for ele in names]), all_metrics))
            return relevant_metrics




    @staticmethod
    def get_max_y(data_points):
        return get_max_y(data_points)


class SparkLogParser:
    prefix_max_len = 21
    r_spark_log = r'.* (?:error|info|warning)(.*)'
    log_types = ['error', 'info', 'warn']

    def __init__(self, log_file, profile_file='', id='', normalize=True):
        if log_file != '':
            self.logfile = log_file
            self.time_pattern, self.re_time_pattern, self.re_app_start, self.re_job_start, self.re_job_end = None, None, None, None, None
            self.re_task_start, self.re_task_end, self.re_spark_log, self.re_problempattern = None, None, None, None
            self.application_name = ''  # application name is always set even if not provided by user
            self.jobs = list()  # [(job_id, job_start, job_end), ...   [(0, 1546683722000, 1546684219000),...
            self.task_intervals = dict()  # dict_items([((0, 0, 0), (1546683722000, 1546684219000)),
            self.stage_intervals = dict()
            self.job_intervals = dict()
            self.identify_timeformat()
        if profile_file is '':
            self.profile_parser = ProfileParser(self.logfile, normalize)
        else:
            self.profile_parser = ProfileParser(profile_file, normalize)
        self.id = id

    def open_stream(self):
        if self.logfile.endswith('gz'):
            return gzip.open(self.logfile, 'rt')
        else:
            return open(self.logfile, 'r')

    def get_available_metrics(self) -> List[str]:
        return self.profile_parser.get_available_metrics()

    def identify_timeformat(self):
        stream = self.open_stream()
        for logline in stream:
            for time_pattern in time_patterns:
                match_attempt = re.match(time_pattern, logline)
                if match_attempt is not None:
                    print('^^ Identified time format for log file: ' + time_patterns[time_pattern])
                    self.time_pattern = time_pattern
                    break
            if self.time_pattern is not None:
                break
        stream.close()

        if self.time_pattern is None:
            print('^^ Warning: log file is empty or has an unknown time format, edit in XXX')
        else:
            self.re_time_pattern = re.compile(self.time_pattern, re.IGNORECASE)
            self.re_app_start = re.compile(self.time_pattern + r_app_start, re.IGNORECASE)
            self.re_job_start = re.compile(self.time_pattern + r_job_start, re.IGNORECASE)
            self.re_job_end = re.compile(self.time_pattern + r_job_end, re.IGNORECASE)
            self.re_task_start = re.compile(self.time_pattern + r_task_start, re.IGNORECASE)
            self.re_task_end = re.compile(self.time_pattern + r_task_end, re.IGNORECASE)
            self.re_spark_log = re.compile(self.time_pattern + r_spark_log, re.IGNORECASE)
            self.re_problempattern = re.compile(self.time_pattern + ' (error|warn)', re.IGNORECASE)

    def extract_time(self, line):
        match_obj = self.re_time_pattern.match(line)
        if match_obj:
            datetime_obj = datetime.strptime(match_obj.group(1), time_patterns[self.time_pattern])
            ms = to_epochms(datetime_obj)
            return ms
        else:
            return None

    def parse_profile(self):  # delegates to embedded ProfileParser
        self.profile_parser.deduce_profiler()

    def get_available_metrics(self):
        return self.profile_parser.get_available_metrics(self.id)

    def __make_graph(self) -> List[Scatter]: # delegates to embedded ProfileParser
        return self.profile_parser.make_graph(self.id)

    def get_metrics(self, names=list()) -> List[Scatter]:
        return self.profile_parser.get_metrics(names)

    def ignore_metrics(self, names=list()) -> List[Scatter]:
        return self.profile_parser.ignore_metrics(names)

    def get_max_y(self, data_points):
        return self.profile_parser.get_max_y(data_points)

    @staticmethod
    def pick_longest_frequent(length_frequ):
        return length_frequ[0] * length_frequ[1], ((length_frequ[0] - length_frequ[1]) * (length_frequ[1] - length_frequ[0]))

    @staticmethod
    def find_reps(elements):
        reps = []
        for prefix_len in range(1, SparkLogParser.prefix_max_len):
            suffix = elements.copy()
            prefix = suffix[:prefix_len]
            repeat = 0  # prefix repeat
            while prefix == suffix[:prefix_len]:
                suffix = suffix[prefix_len:]
                repeat += 1

            if repeat >= 2:
                reps.append((prefix_len, repeat))
        return reps

    @staticmethod
    def collapse(log, rank=False) -> List[Tuple[int, List[str]]]:
        collapsed_log = []
        is_last_pref = False

        while len(log) > 0:
            candidates = SparkLogParser.find_reps(log)
            if len(candidates) >= 1:
                prefix_len, repeats = max(candidates, key=lambda ele: SparkLogParser.pick_longest_frequent(ele))
                collapsed_log.append((repeats, log[:prefix_len]))
                log = log[prefix_len * repeats:]
                is_last_pref = False
            else:
                curr_prefix = log[:1]
                log = log[1:]
                if is_last_pref:
                    collapsed_log[-1] = (1, collapsed_log[-1][1] + curr_prefix)  # end of list => append to previous prefix
                else:
                    collapsed_log.append((1, curr_prefix))  # penultimate
                    is_last_pref = True
        if rank:
            collapsed_log.sort(key=lambda segment: -segment[0])
            return collapsed_log
        else:   # only take strings and flatten list of lists
            return reduce(operator.concat, (map(lambda entry: entry[1], collapsed_log)))

    @staticmethod
    def digest_string(string) -> str:
        shrinked = re.sub(r'[^a-z]', '', string.lower())
        # hashed = hashlib.md5()
        hashed = hashlib.sha1()
        hashed.update(shrinked.encode())
        return hashed.hexdigest()

    @staticmethod
    def digest_strings(string_list) -> str:
        return SparkLogParser.digest_string(''.join(string_list))

    @staticmethod
    def dedupe_errors(stack) -> Deque[Tuple[int, List[str]]]:
        digests = set()
        collapsed_errors = deque()
        while len(stack) > 0:
            last_lines = stack.pop()
            digested_line = SparkLogParser.digest_strings(last_lines)
            if digested_line not in digests:
                collapsed_errors.append(last_lines)
                digests.add(digested_line)
        return collapsed_errors

    @staticmethod
    def dedupe_source_errors(stack) -> Deque[Tuple[str, List[str]]]:
        digests = set()
        collapsed_errors = deque()
        while len(stack) > 0:
            lastele = stack.pop()
            last_file = lastele[0]
            last_lines = lastele[1]
            digested_line = SparkLogParser.digest_strings(last_lines)
            if digested_line not in digests:
                collapsed_errors.append((last_file, last_lines))
                digests.add(digested_line)
        return collapsed_errors

    def get_top_log_chunks(self, log_level='') -> List[Tuple[int, List[str]]]:
        log_types_to_process = self.log_types
        if log_level != '':
            log_types_to_process = [log_level.lower()]

        log_contents = list()
        stream = self.open_stream()
        for line in stream:
            match_obj = self.re_spark_log.match(line.strip())
            if match_obj:
                line_type = match_obj.group(2)
                if line_type.lower() in log_types_to_process:
                    log_contents.append(match_obj.group(3).strip())
        stream.close()
        collapsed_ranked_log = SparkLogParser.collapse(log_contents, rank=True)
        return collapsed_ranked_log

    def extract_errors(self, deduplicate=True) -> Deque[Tuple[int, List[str]]]:
        stream = self.open_stream()
        in_multiline = False
        errors = list()
        multiline_message = list()

        for logline in stream:
            logline = logline.strip()
            match_obj = self.re_problempattern.match(logline)
            if in_multiline:
                normal_match_obj = self.re_spark_log.match(logline)
                if normal_match_obj:  # new logline so close the previous multiline error one
                    errors.append(multiline_message.copy())
                    multiline_message.clear()
                    in_multiline = False
                else:  # continued multiline error
                    multiline_message.append(logline)
            if match_obj:
                in_multiline = True
                multiline_message.append(logline)
        if in_multiline:  # Error at end of file
            errors.append(multiline_message.copy())
            multiline_message.clear()
        stream.close()
        #  Collapse log messages internally for repeated segments
        collapsed_errors = deque(map(lambda entry: SparkLogParser.collapse(entry), errors))
        if deduplicate:
            collapsed_errors = SparkLogParser.dedupe_errors(collapsed_errors)
        return collapsed_errors

    def extract_entity_id(self, match_obj, entity):
        timestamp = match_obj.group(1)  # 2018-12-26 12:05
        datetime_obj = datetime.strptime(timestamp, time_patterns[self.time_pattern])
        ms = to_epochms(datetime_obj)

        if entity == 'task':
            task_id = match_obj.group(2)
            stage_id = match_obj.group(3)
            task_stage_id = (float(task_id), float(stage_id))
            return task_stage_id, ms
        elif entity == 'job':
            job_id = match_obj.group(2)
            return int(job_id), ms
        else:
            raise Exception('Unknown entity type: ' + entity)

    # extracts task start & endpoints
    def extract_task_intervals(self):
        stream = self.open_stream()
        start_times = dict()
        end_times = dict()

        for line in stream:
            line = line.strip()

            match_obj = self.re_app_start.match(line)
            if match_obj:
                name = match_obj.group(2)
                if self.application_name != '':
                    raise Exception('Several Spark applications wrote to the same file: ' + self.logfile)
                else:
                    self.application_name = name
                    continue

            # Extracting jobs
            match_obj = self.re_job_start.match(line)
            if match_obj:
                job_id, ms = self.extract_entity_id(match_obj, 'job')
                if len(self.jobs) > 0:
                    (previous_job_id, _, _) = self.jobs[-1]
                    if previous_job_id == job_id:
                        raise Exception('Conflicting info for start/end of job ' + job_id)
                self.jobs.append((job_id, ms, -1))  # set job end time below
                continue
            match_obj = self.re_job_end.match(line)
            if match_obj:
                job_id, ms = self.extract_entity_id(match_obj, 'job')
                (previous_job_id, job_start, dummy_end) = self.jobs.pop()
                if job_id != previous_job_id or dummy_end != -1:
                    raise Exception('Conflicting info for start/end of job ' + job_id + 'and ' + previous_job_id)
                self.jobs.append((job_id, job_start, ms))
                continue

            # Extracting task/stage/job ids with start/endtimes
            match_obj = self.re_task_start.match(line)
            if match_obj:
                task_stage_id, ms = self.extract_entity_id(match_obj, 'task')
                active_job_id = '' #  Executor logs don't have a Job ID Spark 2.4
                if len(self.jobs) > 0:
                    (active_job_id, _, dummy) = self.jobs[-1]
                    if dummy != -1:
                        raise Exception('Conflicting info for start/end of job ' + active_job_id + 'and task' + task_stage_id)
                task_stage_job_id = (task_stage_id[0], task_stage_id[1], active_job_id)
                start_times[task_stage_job_id] = ms
                continue

            match_obj = self.re_task_end.match(line)
            if match_obj:
                task_stage_id, ms = self.extract_entity_id(match_obj, 'task')
                active_job_id = ''  # Executor logs don't have a Job ID Spark 2.4
                if len(self.jobs) > 0:
                    (active_job_id, _, dummy) = self.jobs[-1]
                    if dummy != -1:
                        raise Exception('Conflicting info for start/end of job ' + active_job_id + 'and task' + task_stage_id)
                task_stage_job_id = (task_stage_id[0], task_stage_id[1], active_job_id)
                end_times[task_stage_job_id] = ms
                continue

        if start_times.keys() != end_times.keys():
            print("^^ Warning: Not all tasks completed successfully: " + str(start_times.keys() - end_times.keys()))
        print('^^ Extracting task intervals')
        for task in start_times.keys():
            if task in end_times:
                self.task_intervals[task] = (start_times[task], end_times[task])

        self.extract_stage_intervals()
        self.extract_job_intervals()
        stream.close()
        return self.task_intervals

    def extract_stage_intervals(self):
        print('^^ Extracting stage intervals')
        stage_intervals = dict()
        if len(self.task_intervals) == 0:
            self.extract_task_intervals()
        for ((_, s_id, job_id), (start, end)) in self.task_intervals.items():
            stage_id = (s_id, job_id)
            if stage_id in stage_intervals:
                (previous_start, previous_end) = stage_intervals[stage_id]
                if start < previous_start:
                    previous_start = start
                if end > previous_end:
                    previous_end = end
                stage_intervals[stage_id] = (previous_start, previous_end)
            else:
                stage_intervals[stage_id] = (start, end)
        self.stage_intervals = stage_intervals
        return self.stage_intervals

    def extract_job_intervals(self):
        print('^^ Extracting job intervals')
        job_intervals = dict()
        if len(self.stage_intervals) == 0:
            self.extract_stage_intervals()
        for ((_, job_id), (start, end)) in self.stage_intervals.items():
            if job_id in job_intervals:
                (previous_start, previous_end) = job_intervals[job_id]
                if start < previous_start:
                    previous_start = start
                if end > previous_end:
                    previous_end = end
                job_intervals[job_id] = (previous_start, previous_end)
            else:
                job_intervals[job_id] = (start, end)
        self.job_intervals = job_intervals
        return self.job_intervals

    def get_job_intervals(self):
        if len(self.job_intervals) == 0:
            self.extract_job_intervals()
        return self.job_intervals

    def extract_active_tasks(self) -> Tuple[List[int], List[int]]:
        if len(self.task_intervals) == 0:
            self.extract_task_intervals()
        # dict_items([((0, 0, 0), (1546683722000, 1546684219000)),
        application_start = min(list(map(lambda x: x[1][0], self.job_intervals.items())))
        application_end = max(list(map(lambda x: x[1][1], self.job_intervals.items())))
        application_duration = int(ms_to_seconds(application_end - application_start))
        print('## Application started at {}, ended at {} and took {}'.format(application_start, application_end, application_duration))

        job_time = []
        active_tasks = []

        for step in range(0, application_duration+1):
            step_time = 1000*step + application_start  # to ms
            active_task = 0
            for ((_, _, _), (task_start, task_end)) in self.task_intervals.items():
                if task_start <= step_time <= task_end:
                    active_task += 1
            job_time.append(step_time)
            active_tasks.append(active_task)
        return job_time, active_tasks

    def get_active_tasks_plot(self):
        job_time, active_tasks = self.extract_active_tasks()
        scatter = go.Scatter(
            name='Active Tasks',
            x=job_time,
            y=active_tasks,
            mode='lines+markers',
            hoverinfo='none',
            line=dict(color='darkblue', width=5)
        )
        return scatter

    def extract_stage_markers(self):
        stage_x = list()
        stage_y = list()
        texts = list()
        if len(self.stage_intervals) == 0:
            self.extract_stage_intervals()

        for ((stage_id, job_id), (start, end)) in self.stage_intervals.items():
            stage_name = '@'.join((str(stage_id), str(job_id)))
            stage_x.append(start)
            texts.append('Stage ' + stage_name + ' start')
            stage_x.append(end)
            texts.append('Stage ' + stage_name + ' end')
            stage_y.append(0)
            stage_y.append(0)

        markers = go.Scatter(
            name="Stage Labels",
            x=stage_x,
            y=stage_y,
            mode='markers+text',
            text=texts,
            textposition='bottom center',
            marker=dict(color='darkblue', size=18),
            opacity=.5
        )

        return markers

    def graph_tasks(self, maximum) -> List[Scatter]:
        data = []
        tasks_x = []
        tasks_y = []
        texts = []
        multiplier = 1
        distance = 0.0
        if len(self.task_intervals) == 0:
            self.extract_task_intervals()
        if maximum < 1.0:
            distance = 1.0 / len(self.task_intervals)

        else:
            distance = maximum / len(self.task_intervals)   # y-distance between stage lines

        for ((task_id, stage_id, job_id), (task_start, task_end)) in self.task_intervals.items():
            task_name = '@'.join((str(task_id), str(stage_id), str(job_id)))
            text = 'Task ' + task_name
            tasks_x.append(task_start)
            tasks_y.append(task_id + stage_id + distance*multiplier)
            # tasks_y.append(distance*multiplier)
            texts.append(text + ' Start')
            #  Create horizontal task lines
            scatter = go.Scatter(
                name=text,
                x=[task_start, task_end],
                y=[task_id + stage_id + distance*multiplier, task_id + stage_id + distance*multiplier],
                # y=[distance*multiplier, distance*multiplier],
                mode='lines+markers',
                hoverinfo='none',
                line=dict(color='darkblue', width=5),
                opacity=0.5
            )
            data.append(scatter)

            tasks_x.append(task_end)
            tasks_y.append(task_id + stage_id + distance*multiplier)
            # tasks_y.append(distance*multiplier)
            texts.append(text + ' End')

            multiplier += 1

        #  Create markers for task start/end points
        trace_tasks = go.Scatter(
            name="Task labels",
            x=tasks_x,
            y=tasks_y,
            mode='markers+text',
            text=texts,
            hoverinfo='none',
            textposition='bottom center',
            marker=dict(color='darkblue', size=14),
            opacity=.5
        )
        data.append(trace_tasks)
        return data

    def extract_job_markers(self, max=10):
        vertical_lines = list()
        if len(self.job_intervals) == 0:
            self.extract_job_intervals()
        for (_, (start, end)) in self.job_intervals.items():
            vertical_lines.append({         # Line Vertical
                'type': 'line',
                'x0': start,
                'y0': 0,
                'x1': start,
                'y1': max,
                'line': { 'color': 'rgb(128, 0, 128)', 'width': 4, 'dash': 'dot', },
            })
            vertical_lines.append({         # Line Vertical
                'type': 'line',
                'x0': end,
                'y0': 0,
                'x1': end,
                'y1': max,
                'line': {'color': 'rgb(128, 0, 128)', 'width': 4, 'dash': 'dot', },
            })

        return {'shapes': vertical_lines}


class AppParser:
    def __init__(self, logs_path, suffix='stderr'):
        self.master_logparser = None
        if logs_path.endswith(os.sep):
            logs_path = logs_path[:-1]
        self.logs_path = logs_path
        logfiles = glob.glob(logs_path + '*/**/' + suffix + '*', recursive=False)
        logfiles.sort(key=lambda path: path)

        # [['container_1547584802630_0001_01_000001', 'stderr.gz']
        suffixes = map(lambda logfile: logfile[len(logs_path) + 1:].split(os.sep), logfiles)
        app_dirs = list(filter(lambda suffix: len(suffix) == 2, suffixes))
        cluster_dirs = list(filter(lambda suffix: len(suffix) == 3, suffixes))

        dummy_id = 1  # artificial ID if path is weird

        if len(app_dirs) > 0 and len(app_dirs) > len(cluster_dirs):
            print('^^ Identified app path with log files')
            self.parsers = list()
            for app_dir in app_dirs:
                loc = os.sep.join(app_dir)
                loc = os.sep.join((logs_path, loc))

                parent = loc[:loc.rindex(os.sep)]
                stdout_glob = glob.glob(parent + '*/' + 'stdout' + '*', recursive=False)  # ToDo: Better logic
                stdout_path = ''
                if len(stdout_glob) > 0:
                    stdout_path = stdout_glob[0]

                re_container_id = re.compile(r_container_id, re.IGNORECASE)
                match_obj = re_container_id.match(loc)
                if match_obj:
                    container_id = match_obj.group(1)
                    self.parsers.append(SparkLogParser(loc, profile_file=stdout_path, id=container_id))
                else:
                    self.parsers.append(SparkLogParser(loc, profile_file=stdout_path, id=dummy_id))
                    dummy_id += 1
        elif len(cluster_dirs) > 0:
            print('^^ Identified cluster job path several apps')
        else:
            raise Exception('Path does not contain log files in known format')
        self.identify_master_log()

    def get_maxima(self) -> Dict[str, float]:
        maxima = {}
        for parser in self.parsers:
            all_metrics: List[Scatter] = parser.profile_parser.get_maxima()
            for metric in all_metrics:
                metric_name = metric.name
                max_value = float(get_max_y([metric]))
                if metric_name in maxima and max_value > maxima[metric_name]:
                    maxima[metric_name] = max_value
        return maxima

    def extract_errors(self) -> Deque[Tuple[str, List[str]]]:
        app_errors = deque()
        log_sources = deque()
        for parser in self.parsers:
            container_error = parser.extract_errors(True)
            app_errors.extend(container_error)
            for _ in range(0, len(container_error)):
                log_sources.append(parser.logfile)

        # sort based on timestamp
        timed_errors = list()
        for app_error in app_errors:
            head = app_error[0]
            ms = self.parsers[0].extract_time(head)
            timed_errors.append((ms, log_sources.popleft(), app_error))

        timed_errors.sort(key=lambda pair: pair[0])
        app_errors = SparkLogParser.dedupe_source_errors(deque(map(lambda triple: (triple[1], triple[2]), timed_errors)))
        return app_errors

    def identify_master_log(self):
        for parser in self.parsers:
            parser.extract_task_intervals()

            if parser.application_name != '':
                self.master_logparser = parser
                break

    def get_master_logfile(self):
        return self.master_logparser.logfile

    def get_master_logparser(self):
        return self.master_logparser

    def get_executor_logparsers(self):
        executors = list()
        for parser in self.parsers:
            if parser != self.master_logparser:
                executors.append(parser)
        return executors

    def graph_tasks(self, maximum):
        if self.master_logparser is None:
            raise Exception('No master log file found for ' + self.logs_path)
        return self.master_logparser.graph_tasks(maximum)

    def extract_stage_markers(self):
        if self.master_logparser is None:
            raise Exception('No master log file found for ' + self.logs_path)
        return self.master_logparser.extract_stage_markers()

    def extract_job_markers(self, max_y=100):
        if self.master_logparser is None:
            raise Exception('No master log file found for ' + self.logs_path)
        return self.master_logparser.extract_job_markers(max_y)


    def get_job_intervals(self):
        if self.master_logparser is None:
            raise Exception('No master log file found for ' + self.logs_path)
        if len(self.job_intervals) == 0:
                self.master_logparser.extract_job_intervals()
        return self.master_logparser.get_job_intervals()

    def get_active_tasks_plot(self):  # Find the master log file and call its function
        if self.master_logparser is None:
            raise Exception('No master log file found for ' + self.logs_path)
        return self.master_logparser.get_active_tasks_plot()

    def extract_job_markers(self, max=10):
        if self.master_logparser is None:
            raise Exception('No master log file found for ' + self.logs_path)
        return self.master_logparser.extract_job_markers(max)


if __name__ == '__main__':
    log_path = '/Users/a/logs/application_1550152404841_0001'
    app_parser = AppParser(log_path)

# Made at https://github.com/g1thubhub/phil_stopwatch by writingphil@gmail.com