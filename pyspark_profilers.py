import os.path
from os.path import join
from sys import _current_frames
from threading import Thread, Event
from pyspark.profiler import BasicProfiler
from pyspark import AccumulatorParam
from collections import deque, defaultdict
import psutil
import time
import json
from typing import DefaultDict, Tuple, List, Set

class CustomProfiler(BasicProfiler):
    def show(self, id):
        print("My custom profiles for RDD:%s" % id)

# A custom profiler has to define or inherit the following methods:
# profile - will produce a system profile of some sort.
# stats - return the collected stats.
# dump - dumps the profiles to a path
# add - adds a profile to the existing accumulated profile

#######################################################################################################################

# Phil PySpark Profiler for CPU & Memory

class CpuMemProfiler(BasicProfiler):
    profile_interval = 0.1  # same default value as in Uber's profiler

    def __init__(self, ctx):
        """ Creates a new accumulator for combining the profiles of different partitions of a stage """
        self._accumulator = ctx.accumulator(list(), CpuMemParam())
        self.profile_interval = float(ctx.environment.get('profile_interval', self.profile_interval))
        self.pids = set()

    def profile(self, spark_action):
        """ Runs and profiles the method to_profile passed in. A profile object is returned. """
        parser = CpuMemParser(self.profile_interval)
        parser.start()
        spark_action()  # trigger the Spark job
        parser.stop()
        self._accumulator.add(parser.profiles)

    def show(self, id):
        """ Print the profile stats to stdout, id is the RDD id """
        print(self.collapse())

    def dump(self, id, path):
        """ Dump the profile into path, id is the RDD id; See Profiler.dump() """
        if not os.path.exists(path):
            print('^^ Path ' + path + ' does not exist, trying to create it' )
            os.makedirs(path)
        self.get_pids()
        for pid in self.pids:
            with open(join(path, 's_{}_{}_cpumem.json'.format(id, pid)), 'w') as file:
                file.write(self.collapse_pid(pid))

    def get_pids(self) -> Set[int]:
        for profile_dict in self._accumulator.value:
            self.pids.add(profile_dict['pid'])
        return self.pids

    def collapse(self) -> str:
        """ Rearrange the result for further processing """
        return '\n'.join([json.dumps(profile_dict) for profile_dict in self._accumulator.value])

    def collapse_pid(self, pid) -> str:
        """ Rearrange the result for further processing split up according to pid"""
        pid_result = []
        for profile_dict in self._accumulator.value:
            if profile_dict['pid'] == pid:
                pid_result.append(json.dumps(profile_dict))
        return '\n'.join(pid_result)


class CpuMemParser(object):
    def __init__(self, profile_interval):
        self.profile_interval = profile_interval
        self.thread = Thread(target=self.catch_mem_cpu)
        self.event = Event()
        self.profiles = []

    def catch_mem_cpu(self):
        while not self.event.is_set():
            self.event.wait(self.profile_interval)
            pid = os.getpid()
            current_process = psutil.Process(pid)
            current_time = int(round(time.time() * 1000))
            # mem_usage =current_process.memory_full_info()  Only +RSS on MacOS
            mem_usage = current_process.memory_info()
            cpu_percent = current_process.cpu_percent(interval=self.profile_interval)
            profile = {'pid': pid, 'epochMillis': current_time, 'pmem_rss': mem_usage.rss, 'pmem_vms': mem_usage.vms, 'pmem_pfaults': mem_usage.pfaults, 'cpu_percent': cpu_percent}
            self.profiles.append(profile)

    def start(self):
        self.thread.start()

    def stop(self):
        self.event.set()
        self.thread.join()


class CpuMemParam(AccumulatorParam):

    def zero(self, value) -> List:
        """ Provide a 'zero value' for the type, compatible in dimensions """
        return list()

    def addInPlace(self, profiles, new_profiles) -> List:
        """ Add 2 values of the accumulator's data type, returning a new value; for efficiency, can also update C{value1} in place and return it. """
        profiles.extend(new_profiles)
        return profiles

#######################################################################################################################

# Phil PySpark Profiler for catching stack traces


class StackProfiler(BasicProfiler):
    profile_interval = 0.1  # same default value as in Uber's profiler

    def __init__(self, ctx):
        """ Creates a new accumulator for combining the profiles of different partitions of a stage """
        self._accumulator = ctx.accumulator(defaultdict(int), StackParam())
        self.profile_interval = float(ctx.environment.get('profile_interval', self.profile_interval))

    def profile(self, spark_action):
        """ Runs and profiles the method to_profile passed in. A profile object is returned. """
        parser = StackParser(self.profile_interval)
        parser.start()
        spark_action()  # trigger the Spark job
        parser.stop()
        self._accumulator.add(parser.stack_frequ)

    def show(self, id):
        """ Print the profile stats to stdout, id is the RDD id """
        print(self.collapse(str(id) + '_id'))

    def dump(self, id, path):
        """ Dump the profile into path, id is the RDD id; See Profiler.dump() """
        if not os.path.exists(path):
            print('^^ Path ' + path + ' does not exist, trying to create it' )
            os.makedirs(path)
        with open(join(path, 's_{}_stack.json'.format(id)), 'w') as file:
            file.write(self.collapse(str(id) + '_id'))

    def collapse(self, id) -> str:
        """ Rearrange the result for further processing """
        stacks = []
        for stack, count in self._accumulator.value.items():
            stacks.append('{"stacktrace":[' + stack + '],"count":' + str(count) + ',"i_d":"' + str(id) + '"}\n')
        return ''.join(stacks)


class StackParser(object):
    def __init__(self, profile_interval):
        self.profile_interval = profile_interval
        self.stack_frequ = defaultdict(int)  # incrementing count values for stackframes in a Map
        self.thread = Thread(target=self.capture_stack)
        self.event = Event()

    @staticmethod
    def parse_stackframe(frame) -> str:
        current_stack = deque()
        while frame is not None:
            linenum = str(frame.f_lineno)  # FrameType
            co_filename = frame.f_code.co_filename  # CodeType
            co_name = frame.f_code.co_name  # CodeType
            current_stack.append('\"' + ":".join((co_filename, co_name, linenum)) + '\"')  # similar output to JVM profiler
            frame = frame.f_back
        return ','.join(current_stack)

    def capture_stack(self):
        while not self.event.is_set():
            self.event.wait(self.profile_interval)
            for thread_id, stackframe in _current_frames().items():  # thread id to T's current stack frame
                if thread_id != self.thread.ident:  # Thread identifier
                    stack = self.parse_stackframe(stackframe)
                    self.stack_frequ[stack] += 1

    def start(self):
        self.thread.start()

    def stop(self):
        self.event.set()
        self.thread.join()


class StackParam(AccumulatorParam):

    def zero(self, value) -> DefaultDict[str, int]:
        """ Provide a 'zero value' for the type, compatible in dimensions """
        return defaultdict(int)

    def addInPlace(self, dict1, dict2) -> DefaultDict[str, int]:
        """ Add 2 values of the accumulator's data type, returning a new value; for efficiency, can also update C{value1} in place and return it. """
        for frame, frequ in dict2.items():
            dict1[frame] += frequ
        return dict1

#######################################################################################################################

# Phil PySpark Profiler, combination of CpuMemProfiler & StackProfiler


class CpuMemStackProfiler(BasicProfiler):
    profile_interval = 0.1  # same default value as in Uber's profiler

    def __init__(self, ctx):
        """ Creates a new accumulator for combining the profiles of different partitions of a stage """
        self._accumulator = ctx.accumulator(tuple((list(), defaultdict(int))), CpuMemStackParam())
        self.profile_interval = float(ctx.environment.get('profile_interval', self.profile_interval))
        self.pids = set()

    def profile(self, spark_action):
        """ Runs and profiles the method to_profile passed in. A profile object is returned. """
        parser = CpuMemStackParser(self.profile_interval)
        parser.start()
        spark_action()  # trigger the Spark job
        parser.stop()
        self._accumulator.add(parser.profiles)

    def show(self, id):
        """ Print the profile stats to stdout, id is the RDD id """
        print(self.collapse(id))


    def collapse(self, id) -> str:
        """ Rearrange the result for further processing """
        results = []
        for profiledict in self._accumulator.value[0]:
            results.append(json.dumps(profiledict) + '\n')
        for stack, count in self._accumulator.value[1].items():
            results.append(''.join((''.join(stack), '\t', str(count), '\t', str(id) + '_id', '\n')))
        return ''.join(results)


    def dump(self, id, path):
        """ Dump the profile into path, id is the RDD id; See Profiler.dump() """
        if not os.path.exists(path):
            print('^^ Path ' + path + ' does not exist, trying to create it' )
            os.makedirs(path)
        self.get_pids()
        for pid in self.pids:
            with open(join(path, 's_{}_{}_cpumem.json'.format(id, pid)), 'w') as file:
                file.write(self.collapse_pid(pid))
        with open(join(path, 's_{}_stack.json'.format(id)), 'w') as file:
            file.write(self.collapse_stacks(str(id) + '_id'))

    def get_pids(self) -> Set[int]:
        for profile_dict in self._accumulator.value[0]:
            self.pids.add(profile_dict['pid'])
        return self.pids

    def collapse_pid(self, pid) -> str:
        pid_result = list()
        for profile_dict in self._accumulator.value[0]:
            if profile_dict['pid'] == pid:
                pid_result.append(json.dumps(profile_dict))
        return '\n'.join(pid_result)

    def collapse_stacks(self, id) -> str:
        results = list()
        for stack, count in self._accumulator.value[1].items():
            results.append('{"stacktrace":[' + stack + '],"count":' + str(count) + ',"i_d":"' + str(id) + '"}\n')
        return ''.join(results)


class CpuMemStackParser(object):
    def __init__(self, profile_interval):
        self.profile_interal = profile_interval
        self.thread = Thread(target=self.catch_mem_cpu_stack)

        self.event = Event()
        self.profiles = tuple((list(), defaultdict(int)))

    def catch_mem_cpu_stack(self):
        while not self.event.is_set():
            self.event.wait(self.profile_interal)
            pid = os.getpid()
            current_process = psutil.Process(pid)
            current_time = int(round(time.time() * 1000))
            # mem_usage =current_process.memory_full_info()
            mem_usage = current_process.memory_info()
            cpu_percent = current_process.cpu_percent(interval=self.profile_interal)
            profile = {'pid': pid, 'epochMillis': current_time, 'pmem_rss': mem_usage.rss, 'pmem_vms': mem_usage.vms, 'pmem_pfaults': mem_usage.pfaults, 'cpu_percent': cpu_percent}
            self.profiles[0].append(profile)
            for thread_id, stackframe in _current_frames().items():  # thread id to T's current stack frame
                if thread_id != self.thread.ident: # Thread identifier
                    stack = self.parse_stackframe(stackframe)
                    self.profiles[1][stack] += 1

    @staticmethod
    def parse_stackframe(frame) -> str:
        current_stack = deque()
        while frame is not None:
            linenum = str(frame.f_lineno)  # FrameType
            co_filename = frame.f_code.co_filename  # CodeType
            co_name = frame.f_code.co_name  # CodeType
            current_stack.append('\"' + ":".join((co_filename, co_name, linenum)) + '\"')  # similar output to JVM profiler
            frame = frame.f_back
        return ','.join(current_stack)

    def start(self):
        self.thread.start()

    def stop(self):
        self.event.set()
        self.thread.join()


class CpuMemStackParam(AccumulatorParam):
    def zero(self, value) -> Tuple[List, DefaultDict[str, int]]:
        """ Provide a 'zero value' for the type, compatible in dimensions """
        return tuple((list(), defaultdict(int)))

    def addInPlace(self, pair1, pair2) -> Tuple[List, DefaultDict[str, int]]:
        """ Add 2 values of the accumulator's data type, returning a new value; for efficiency, can also update C{value1} in place and return it. """
        if len(pair2[0]) > 0:
            pair1[0].extend(pair2[0])
        for frame, frequ in pair2[1].items():
            pair1[1][frame] += frequ
        return pair1


#######################################################################################################################

# Profiler map for command line args, default is CpuMemProfiler
profiler_map = {'customprofiler': CustomProfiler, 'cpumemprofiler': CpuMemProfiler, 'cpumem': CpuMemProfiler,
             'stackprofiler': StackProfiler, 'stack': StackProfiler, '': CpuMemStackProfiler, 'both': CpuMemStackProfiler,
                'cpumemstack': CpuMemStackProfiler, 'stackcpumem': CpuMemStackProfiler}
