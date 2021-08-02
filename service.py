import dataclasses
import json
import os
import signal
import subprocess
import threading
import time
import sys

from queue import Queue
from configparser import ConfigParser
from typing import Optional, Dict

import psutil


class Process(subprocess.Popen):
    pass


@dataclasses.dataclass
class ProcessData:
    process: Process
    pid: int = None
    balancer: bool = False
    memory_limit_value: int = None
    memory_limit_percent: int = None

    def is_memory_usage_overloaded(self, total_system_memory) -> bool:
        if not self.memory_limit_value and not self.memory_limit_percent:
            return False

        process_memory = psutil.Process(self.pid).memory_info()

        if self.memory_limit_value and process_memory.rss > self.memory_limit_value:
            return True

        if self.memory_limit_percent and self.memory_limit_percent > 0:
            max_process_memory = int(total_system_memory / (100 / self.memory_limit_percent))
            if process_memory.rss > max_process_memory:
                return True

        return False


@dataclasses.dataclass
class RestartQueueItem:
    process_index: int
    time: float


class ServiceHandler:
    STOPPING: bool = False
    processes: Dict[int, ProcessData] = {}
    mutex: threading.Lock = threading.Lock()
    process_restart_queue: 'Queue[Optional[RestartQueueItem]]' = Queue()
    process_monitoring_time: Optional[float] = None


def _gracefully_stop_process(process: Process, timeout: int = 2):
    process.send_signal(signal.SIGTERM)

    try:
        process.wait(timeout)
    except subprocess.TimeoutExpired:
        print("PROCESS NOT CLOSED, KILL IT")
        process.send_signal(signal.SIGKILL)


def _stop(handler: ServiceHandler):
    handler.STOPPING = True

    for id, process_data in handler.processes.items():
        _gracefully_stop_process(process_data.process)


def setup_signal_handlers(handler: ServiceHandler):
    signal.signal(signal.SIGINT, lambda x, y: _stop(handler))
    signal.signal(signal.SIGTERM, lambda x, y: _stop(handler))


def _run_service(service_label: str, index: int = None) -> Process:
    if index:
        arguments = [f'--index={index}']
        cmd = [sys.executable, 'worker.py'] + arguments
        print('[SERVICE START] worker: %s %s' % (service_label, json.dumps(arguments)))
    else:
        cmd = [sys.executable, 'balancer.py']
        print('[SERVICE START] balancer')

    return Process(cmd, stdin=None, stdout=None, stderr=None)


def _wait_for_stopped_child(handler: ServiceHandler) -> Optional[int]:
    pid = None

    try:
        pid, *_ = os.waitid(os.P_ALL, 0, os.WEXITED)
    except ChildProcessError as exc:
        if exc.errno == 10:
            if len(handler.processes) == 0:
                raise RuntimeError("No child process left") from exc
        else:
            print('[CHILD PROCESS ERROR]')
            print(exc)

    return pid


def _restart_process(process: Process) -> Process:
    print('[RESTART PROCESS] cmd: %s', process.args)
    return Process(process.args, stdin=None, stdout=None, stderr=None)


def _add_process_to_restart_queue(handler: ServiceHandler, process_index: int):
    print('[PROCESS STOPPED] pid: %s, trying to start again in 5 sec.' % handler.processes[process_index].pid)
    handler.process_restart_queue.put(RestartQueueItem(process_index, time.monotonic() + 5))


def _process_restart_monitoring(handler: ServiceHandler):
    while True:
        if handler.process_restart_queue.empty():
            time.sleep(1)
            continue

        with handler.mutex:
            item = handler.process_restart_queue.get()

            if item is None:
                break

            if item.time > time.monotonic():
                handler.process_restart_queue.put(item)
            else:
                new_process = _restart_process(handler.processes[item.process_index].process)
                print('[PROCESS RESTARTED] pid: %s' % new_process.pid)
                handler.processes[item.process_index].process = new_process
                handler.processes[item.process_index].pid = new_process.pid

    print('[PROCESS RESTART MONITORING STOPPED]')


def _process_memory_monitoring(handler: ServiceHandler):
    monitoring_period = 300
    with handler.mutex:
        handler.process_monitoring_time = time.monotonic() + monitoring_period

    while handler.process_monitoring_time:

        if not handler.process_monitoring_time or time.monotonic() < handler.process_monitoring_time:
            time.sleep(1)
            continue

        with handler.mutex:
            handler.process_monitoring_time = time.monotonic() + monitoring_period
        total_system_memory = psutil.virtual_memory().total

        for process_index, process_data in list(handler.processes.items()):
            if process_data.is_memory_usage_overloaded(total_system_memory):
                print('[PROCESS RESTARTING] pid %s, memory usage overloaded' % process_data.pid)
                _gracefully_stop_process(process_data.process)

    print('[PROCESS MEMORY MONITORING STOPPED]')


def _start_process_monitoring(handler: ServiceHandler):
    threading.Thread(target=_process_restart_monitoring, args=(handler,)).start()
    threading.Thread(target=_process_memory_monitoring, args=(handler,)).start()


def main(config: ConfigParser):
    handler = ServiceHandler()

    setup_signal_handlers(handler)

    process_index = 0
    for section, section_config in config.items():
        balancer = False
        if section == config.default_section:
            balancer = True
            process = _run_service(section)
        else:
            process_index += 1
            process = _run_service(section, process_index)

        process_data = ProcessData(process=process, pid=process.pid, balancer=balancer)
        process_data.memory_limit_value = section_config.getint('max-process-memory-size')
        process_data.memory_limit_percent = section_config.getint('max-process-memory-size-percent')
        handler.processes[process_index] = process_data

    _start_process_monitoring(handler)

    while not handler.STOPPING:
        pid = _wait_for_stopped_child(handler)
        if pid is None:
            continue

        for index, process_data in handler.processes.items():
            if process_data.pid == pid:
                with handler.mutex:
                    process_data.pid = None
                _add_process_to_restart_queue(handler, index)
                break

    with handler.mutex:
        handler.process_restart_queue.put(None)
        handler.process_monitoring_time = None

    print('[SHUTDOWN]')


if __name__ == '__main__':
    config = ConfigParser()
    config.read('service.ini')

    main(config)
