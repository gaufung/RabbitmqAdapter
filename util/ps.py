# -*- coding:utf-8 -*-
"""
    process and system utilities
    wrapping psutil package
"""
from __future__ import unicode_literals
from __future__ import print_function
import threading
import time
import sys
from collections import namedtuple, OrderedDict
import signal
import psutil

_Resource = namedtuple('Resource', ['cpu_avg', 'cpu_max', 'mem_avg', 'mem_max'])

_ProcessCpuTimes = namedtuple('ProcessCpuTimes', ['user', 'system', 'children_user', 'children_system'])


def _memory_items():
    if sys.platform.startswith("linux"):
        return ["rss", "vms", "shared", "text", "lib",  "data", "dirty"]
    elif sys.platform.startswith("darwin"):
        return ['rss', 'vms', 'pfaults', 'pageins']
    else:
        ['rss', 'vms', 'num_page_faults', 'peak_wset', 'wset', 'peak_paged_pool', 'paged_pool', 'peak_nonpaged_pool', 'nonpaged_pool', 'pagefile', 'peak_pagefile', 'private']


_ProcessMemory = namedtuple("ProcessMemory", _memory_items())


class _ProcessCpuTimesX(_ProcessCpuTimes):
    """
    extension of ProcessCpuTimes by overloading -, + and  += operations
    """
    def __new__(cls, *args, **kwargs):
        return (super, cls).__new__(cls, *args, **kwargs)

    def __sub__(self, other):
        values = [getattr(self, key) - getattr(other, key) for key in self._fields]
        return _ProcessCpuTimesX(values)

    def __add__(self, other):
        values = [getattr(self, key) + getattr(other, key) for key in self._fields]
        return _ProcessCpuTimesX(values)

    def __iadd__(self, other):
        values = [getattr(self, key) + getattr(other, key) for key in self._fields]
        self = _ProcessCpuTimesX(values)
        return self

    def average(self, time_interval):
        if not time_interval:
            return 0
        return float(self.user + self.system) / time_interval * 100


class _ProcessMemoryX(_ProcessMemory):
    """
    extension of ProcessMemory by overloading + operations
    """
    def __new__(cls, *args, **kwargs):
        return (super, cls).__new__(cls, *args, **kwargs)

    def __add__(self, other):
        values = [getattr(self, key) + getattr(other, key) for key in self._fields]
        return _ProcessMemoryX(values)

    def __iadd__(self, other):
        values = [getattr(self, key) + getattr(other, key) for key in self._fields]
        self = _ProcessMemoryX(values)
        return self


class _SystemResource(object):
    """
    System Resources
    """
    def __init__(self):
        self._lock = threading.Lock()
        self._monitor_dict = OrderedDict()
        self._snapshots = []

    def take_snapshot(self):
        """
        take snapshot for system resource of cpu and memory
        :return:
        """
        with self._lock:
            cpu = _ProcessCpuTimesX([0, 0, 0, 0])
            memory = _ProcessMemoryX([0] * len(_memory_items()))
            for pid in psutil.pids():
                try:
                    process = psutil.Process(pid)
                    cpu_times = process.cpu_times()
                    cpu += _ProcessCpuTimesX([cpu_times.user, 0, cpu_times.system, 0])
                    memory += process.memory_info()
                except psutil.NoSuchProcess:
                    pass
                except Exception:
                    pass
            self._snapshots.append((time.time(), cpu, memory))

    def _get_index(self, threshold_timestamp):
        for index, (time_stamp, _, _) in enumerate(self._snapshots):
            if time_stamp >= threshold_timestamp:
                return index
        return len(self._snapshots)

    def clear_expired_snapshot(self, index_threshold=1000):
        """
        clear expire snapshot
        :param index_threshold: index threshold
        :return: None
        """
        with self._lock:
            for monitor_name, monitor_timestamp in self._monitor_dict.iteritems():
                index = self._get_index(monitor_timestamp)
                if index >= index_threshold:
                    self._snapshots = self._snapshots[index:]
                break

    def set_sample(self, name):
        """
        set sample point
        :param name: sample point name
        :return: None
        """
        with self._lock:
            if name not in self._monitor_dict:
                self._monitor_dict[name] = time.time()

    def get_sample(self, name):
        """
        get sample point snapshot
        :param name: sample point name
        :return: a namedtuple:  Resource ['cpu_avg', 'cpu_max', 'mem_avg', 'mem_max']
        """
        with self._lock:
            if name not in self._monitor_dict:
                return
            timestamp = self._monitor_dict[name]
            begin_index = self._get_index(timestamp)
            res_list = self._snapshots[begin_index:]
            del self._monitor_dict[name]
        if not res_list:
            return _Resource(0, 0, 0, 0)
        elif len(res_list) == 1:
            mem_avg = mem_max = res_list[0][2].rss >> 20
            return _Resource(0, 0, mem_avg, mem_max)
        else:
            cpu_max = cpu_avg = (res_list[-1][1] - res_list[0][1]).average(res_list[-1][0] - res_list[0][0])
            mems = [res[2].rss >> 20 for res in res_list]
            mem_avg = sum(mems) / len(res_list)
            mem_max = max(mems)
            return _Resource(cpu_avg, cpu_max, mem_avg, mem_max)

    @staticmethod
    def cpu_snapshot():
        return psutil.cpu_times()

    @staticmethod
    def mem_snapshot():
        return psutil.virtual_memory()


class _SystemResourceThread(threading.Thread):
    """
    worker for system resource monitor
    """
    def __init__(self, interval_time=5, clear_interval=10000):
        super(_SystemResourceThread, self).__init__()
        self._sys_res = _SystemResource()
        self._interval_time = interval_time
        self._flag = True
        self._clear_interval = clear_interval
        self._stop_event = threading.Event()

    def _terminate(self):
        self._flag = False

    def run(self):
        count = 0
        while self._flag:
            self._sys_res.take_snapshot()
            count += 1
            if count % self._clear_interval == 0:
                self._sys_res.clear_expired_snapshot()
            time.sleep(self._interval_time)

    @property
    def resource(self):
        return self._sys_res

    def start(self):
        super(_SystemResourceThread, self).start()

        def on_terminate():
            self._terminate()
            self.join()
        signal.signal(signal.SIGTERM, on_terminate)


system_monitor = _SystemResourceThread()
