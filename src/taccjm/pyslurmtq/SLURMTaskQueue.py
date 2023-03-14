"""
SLURMTaskQueue

Defines classes for executing a collection of tasks in a single SLURM job. A
task is defined as a command to be run in parallel using a given number of
SLURM tasks, as would be run with `ibrun -n`.
"""

import argparse as ap
import copy
import json
import linecache as lc
import logging
import os
import pdb
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import List

from prettytable import PrettyTable
from pythonjsonlogger import jsonlogger

from taccjm.pyslurmtq.Slot import Slot
from taccjm.pyslurmtq.Task import Task
from taccjm.pyslurmtq.utils import (compact_int_list, expand_int_list,
                                    filter_res)

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


class SLURMTaskQueue:
    """
    Object that does the maintains a list of Task objects.
    This is internally created inside a ``LauncherJob`` object.


    Attributes
    ----------
    task_slots : List(:class:Slot)
        List of task slots available. This is parsed upon initialization from
        SLURM environment variables SLURM_JOB_NODELIST and SLURM_TASKS_PER_NODE.
    workdir : str
        Path to directory to store files for tasks executed, if the tasks
        themselves dont specify their own work directories. Defaults to a
        directory with the prefix `.stq-job{SLURM_JOB_ID}-` in the
        current working directory.
    delay : float
        Number of seconds to pause between iterations of updating the queue.
        Default is 1 second. Note this affects the poll rate of tasks runing
        in the queue.
    task_max_runtime : float
        Max run time, in seconds, any individual task in the queue can run for.
    max_runtime : float
        Max run time, in seconds, for execution of `run()` to empty the queue.
    task_count : int
        Running counter, starting from 0, of total tasks that pass through the
        queue. The current count is used for the task_id of the next task added
        to the queue, so that a tasks task_id corresponds to the order in which
        it was added to the queue.
    running_time : float
        Total running time of the queue when `run()` is executed.
    queue : List(:class:Task)
        List of :class:Task in queue. Populated via the
        `enqueue_from_json()` method.
    running : List(:clas:Task)
        List of :class:Task that are currently running.
    completed : List(:clas:Task)
        List of :class:Task that are completed running successfully, in
        that the process executing them returned a 0 exit code.
    errored : List(:clas:Task)
        List of :class:Task that failed to run successfully in that the
        processes executing them returned a non-zero exit code..
    timed_out : List(:clas:Task)
        List of :class:Task that failed to run successfully in that the
        their runtime exceeded `task_max_runtime`.
    invalid : List(:clas:Task)
        List of :class:Task that were not run because their configurations
        were invalid, or the amount of resources required to run them was too
        large.
    """

    def __init__(
        self,
        tasks: List[dict] = None,
        task_file: str = None,
        workdir: str = None,
        task_max_runtime: float = 1e10,
        max_runtime: float = 1e10,
        delay: float = 1,
        loglevel: int = logging.DEBUG,
        summary_interval: float = 60,
    ):
        # Default workdir for executing tasks if task doesn't specify workdir
        self.workdir = workdir
        if self.workdir is None:
            self.workdir = Path(
                tempfile.mkdtemp(
                    prefix=f'.stq-job{os.environ["SLURM_JOB_ID"]}-',
                    dir=Path.cwd(),
                )
            )
        else:
            self.workdir = Path(workdir) if type(workdir) != Path else workdir
            self.workdir.mkdir(exist_ok=True)

        # Set-up job logging
        self._logger = logging.getLogger(__name__)
        _logHandler = logging.FileHandler(self.workdir / "tq_log")
        _formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s - %(levelname)s:%(message)s"
        )
        _logHandler.setFormatter(_formatter)
        self._logger.addHandler(_logHandler)
        self._logger.setLevel(loglevel)

        # Node list - Initialize from SLURM environment
        self.task_slots = []
        self._init_task_slots()

        # Set queue runtime constants
        self.delay = delay
        self.task_max_runtime = task_max_runtime
        self.max_runtime = max_runtime
        self.summary_interval = summary_interval

        # Initialize Task Queue Arrays
        self.task_count = 0
        self.running_time = 0.0
        self.queue = []
        self.running = []
        self.completed = []
        self.errored = []
        self.timed_out = []
        self.invalid = []

        # Enqueue tasks from json file
        if task_file is not None:
            self.enqueue_from_json(task_file)
        if tasks is not None:
            self.enqueue(tasks)

        self._logger.info(f"Queue initialized: {self}", extra=self.__dict__)

    def __str__(self):
        queue_str = ""
        sc = lambda x: compact_int_list(sorted([t.task_id for t in x]))
        if len(self.queue) > 0:
            queue_str += f"queued=[{sc(self.queue)}], "
        if len(self.running) > 0:
            queue_str += f"running=[{sc(self.running)}], "
        if len(self.completed) > 0:
            queue_str += f"completed=[{sc(self.completed)}], "
        if len(self.timed_out) > 0:
            queue_str += f"timed_out=[{sc(self.timed_out)}], "
        if len(self.errored) > 0:
            queue_str += f"errored=[{sc(self.errored)}, "
        if len(self.invalid) > 0:
            queue_str += f"invalid=[{sc(self.invalid)}, "
        queue_str = queue_str[:-2] if len(queue_str) != 0 else queue_str

        unique_slots = list(set([s.host for s in self.task_slots]))
        status = []
        for h in unique_slots:
            free = []
            busy = []
            for s in self.task_slots:
                if s.host == h:
                    free.append(s.idx) if s.is_free() else busy.append(s.idx)
            status.append((h, compact_int_list(free), compact_int_list(busy)))
        slots = [f"{x[0]}: (FREE: [{x[1]}], BUSY: [{x[2]}])" for x in status]

        s = f"(workdir: {self.workdir}, "
        s += f"slots: [{', '.join(slots)}], "
        s += f"queue-state:[{queue_str}])"

        return s

    def _init_task_slots(self):
        """Initialize available task slots from SLURM environment variables"""
        hl = []
        slurm_nodelist = os.environ["SLURM_JOB_NODELIST"]
        self._logger.debug(
            f"Parsing SLURM_JOB_NODELIST {slurm_nodelist}",
            extra={"SLURM_JOB_NODELIST": slurm_nodelist},
        )
        host_groups = re.split(r",\s*(?![^\[\]]*\])", slurm_nodelist)
        for hg in host_groups:
            splt = hg.split("-")
            h = splt[0] if type(splt) == list else splt
            ns = "-".join(splt[1:])
            ns = ns[1:-1] if ns[0] == "[" else ns
            padding = min([len(x) for x in re.split(r"[,-]", ns)])
            hl += [f"{h}-{str(x).zfill(padding)}" for x in expand_int_list(ns)]
        self._logger.debug(f"Parsed nodelist {hl}", extra={"hl": hl})

        tasks_per_host = []
        slurm_tph = os.environ["SLURM_TASKS_PER_NODE"]
        self._logger.debug(f"Parsing SLURM_TAKS_PER_NODE {slurm_tph}")
        total_idx = 0
        for idx, tph in enumerate(slurm_tph.split(",")):
            mult_split = tph.split("(x")
            ntasks = int(mult_split[0])
            if len(mult_split) > 1:
                for i in range(int(mult_split[1][:-1])):
                    tasks_per_host.append(ntasks)
                    for j in range(ntasks):
                        self.task_slots.append(Slot(hl[idx], total_idx + j))
                        self._logger.debug(f"Initialized slot {self.task_slots[-1]}")
                    total_idx += ntasks
            else:
                for j in range(ntasks):
                    self.task_slots.append(Slot(hl[idx], total_idx + j))
                    self._logger.debug(f"Initialized slot {self.task_slots[-1]}")
                total_idx += ntasks
        self._logger.debug(f"Initialized {len(self.task_slots)}")

    def _request_slots(self, task):
        """Request a number of slots for a task"""
        start = 0
        found = False
        cores = task.cores
        while not found:
            if start + cores > len(self.task_slots):
                return False
            for i in range(start, start + cores):
                found = self.task_slots[i].is_free()
                if not found:
                    start = i + 1
                    break

        # If reach here -> Execute task on offset equal to start
        self._logger.debug(
            f"Starting {task.task_id} at slot index {start}", extra=task.__dict__
        )
        task.execute(start, cores)
        self._logger.info(
            f"{task.task_id} running on process {task.sub_proc.pid}",
            extra=task.__dict__,
        )

        # Mark slots as occupied with with task_id
        for n in range(start, start + cores):
            s = self.task_slots[n]
            self._logger.debug(f"Occupying slot{s}", extra=s.__dict__)
            s.occupy(task)
            self._logger.debug(f"Slot{s} occupied", extra=s.__dict__)

        return True

    def _release_slots(self, task_id):
        """Given a task id, release the slots that are associated with it"""
        for s in self.task_slots:
            if not s.is_free():
                if s.tasks[-1].task_id == task_id:
                    self._logger.debug(f"Releasing slot {s}", extra=s.__dict__)
                    s.release()
                    self._logger.debug(f"Slot {s} released", extra=s.__dict__)

    def _start_queued(self):
        """
        Start queued tasks. For all queued, try to find a continuous set of
        slots equal to the number of cores required for the task. The tasks are
        looped through in decreasing order of number of cores required. If the
        task is to big for the whole set of available slots, it is automatically
        added to the invalid list. Otherwise `_request_slots` is called to see
        if there space for the task to be run in the available slots.
        """
        # Sort queue in decreasing order of # of cores
        tqueue = copy.copy(self.queue)
        tqueue.sort(key=lambda x: -x.cores)
        for task in tqueue:
            if task.cores > len(self.task_slots):
                self._logger.warning(
                    f"Task {task} to large. Adding to invalid list.",
                    extra=task.__dict__,
                )
                task.err_msg = "Invalid task (too many cores for queue): "
                task.err_msg += "{task.cores}>len(self.task_slots)"
                self.queue.remove(task)
                self.invalid.append(task)
                continue
            if self._request_slots(task):
                self._logger.info(
                    f"Successfully found resources for task {task}", extra=task.__dict__
                )
                self.queue.remove(task)
                self.running.append(task)
            else:
                self._logger.debug(
                    f"Unable to find resources for {task}.", extra=task.__dict__
                )

        num_removed = len(tqueue) - len(self.queue)
        if num_removed > 0:
            self._logger.info(f"Started {num_removed} tasks", extra=self.__dict__)

    def _terminate_and_release(self, task, msg):
        """Teriminate a task and release its resources"""
        self._logger.error(msg, extra=task.__dict__)
        task.terminate()
        task.err_msg = msg
        self.timed_out.append(task)
        self._logger.info(f"Releasing slots related assigned to task {task.task_id}")
        self._release_slots(task.task_id)

    def _update(self):
        """
        Update status of tasks in queue by calling polling subprocesses
        executing them with `get_rc()`. Tasks are added to the erorred or
        completed lists, or terminated and added to timed_out list if
        `task_max_runtime` is exceeded.
        """
        running = []
        for t in self.running:
            rc = t.get_rc()
            if rc is None:
                rt = time.time() - t.start_ts
                if rt > self.task_max_runtime:
                    msg = f"Task {t.task_id} has exceeded task max runtime {rt}"
                    self._terminate_and_release(t, msg)
                else:
                    running.append(t)
            else:
                if rc == 0:
                    self._logger.info(
                        f"{t.task_id} DONE: {t.running_time:5.3f}s", extra=t.__dict__
                    )
                    self.completed.append(t)
                    self._release_slots(t.task_id)
                else:
                    msg = f"{t.task_id} FAILED: rt = {t.running_time:5.3f}s, "
                    msg += f"rc = {t.rc}, err file (last_line) = {t.err_msg}"
                    self._logger.error(msg, extra=t.__dict__)
                    self.errored.append(t)
                    self._release_slots(t.task_id)

        # Release slots for completed tasks
        finished = len(self.running) - len(running)
        if finished > 0:
            self.running = running
            self._logger.info("{finished} tasks finished", extra=self.__dict__)

    def _save_summary(self):
        """Save task and queue summaries to workdir"""
        _ = self.summary_by_task(
            print_res=False, fname=str(self.workdir / "task_summary.txt")
        )
        _ = self.summary_by_slot(
            print_res=False, fname=str(self.workdir / "slot_summary.txt")
        )

    def enqueue(self, task_list: List[dict], cores: int = 1):
        """
        Add a list of tasks to the queue. Each task is a dictionary  with at mininum
        each containing a `cmnd` field indicating the command to be executed in parallel
        using a corresponding number of `cores`, which defaults to the passed in value
        if not specified per task configuration.

        Parameters
        ----------
        task_list : List[dict]
            List of dictionaries, one per task with the following fields:
                'cmnd' : required, parllalel command to execute
                'cores' : optional, number of cores to user on this task
                'pre_process' : optional, serial command to run prior to running 'cmnd'
                'post_process' : optional, serial command to run after running 'cmnd'
        cores : int
            Default number of cores to use for each task if not specified within
            task configuration.

        """
        self._logger.debug(f"Enqueuing {len(task_list)} tasks.")

        for i, t in enumerate(task_list):
            self._logger.debug(f"Attempting to create task {self.task_count}", extra=t)
            try:
                task = Task(
                    self.task_count,
                    t.pop("cmnd", None),
                    t.pop("workdir", self.workdir),
                    t.pop("cores", cores),
                    t.pop("pre", None),
                    t.pop("post", None),
                    t.pop("cdir", None),
                )
            except ValueError as v:
                self._logger.error(f"Bad task in list at idx {i}: {v}", extra=t)
                continue
            self._logger.debug(f"Enqueing {task}", extra=task.__dict__)
            self.queue.append(task)
            self.task_count += 1

    def enqueue_from_json(self, filename, cores=1):
        """
        Add a list of tasks to the queue from a JSON file. The json file must
        contain a list of configurations, with at mininum each containing a
        `cmnd` field indicating the command to be executed in parallel using
        a corresponding number of `cores`, which defaults to the passed in value
        if not specified per task configuration.

        Parameters
        ----------
        filename : str
            Path to json files containing list of json configurations, one per
            task to add to the queue.
        cores : int
            Default number of cores to use for each task if not specified within
            task configuration.

        """
        self._logger.debug(f"Loading json task file {filename}")
        with open(filename, "r") as fp:
            task_list = json.load(fp)
        self._logger.debug(f"Found {len(task_list)} tasks.")
        self.enqueue(task_list, cores=cores)

    def run(self):
        """
        Runs tasks and wait for all tasks in queue to complete, or until
        `max_runtime` is exceeded.
        """
        self.start_ts = time.time()
        self._logger.info("Starting launcher job", extra=self.__dict__)
        self._save_summary()
        summary_counter = time.time()
        while True:
            elapsed = time.time() - self.start_ts

            if elapsed - summary_counter > self.summary_interval:
                self._save_summary()
                summary_counter = elapsed

            if elapsed >= self.max_runtime:
                msg = f"Exceeded max runtime : {elapsed}>{self.max_runtime}"
                self._logger.info(msg, extra=self.__dict__)
                for t in self.running:
                    self._terminate_and_release(t, msg)
                break

            # Start queued jobs
            self._logger.debug("Starting queued tasks")
            self._start_queued()

            # Update queue for completed/errored jobs
            self._logger.debug("Updating task lists")
            self._update()

            # Wait for a bit
            time.sleep(self.delay)

            # Check if done
            if len(self.running) == 0:
                if len(self.queue) == 0:
                    self._logger.info(f"Running and queue are empty.")
                    break

        self.running_time = time.time() - self.start_ts
        self._logger.info("Queue run finished", extra=self.__dict__)
        self._save_summary()

    def read_log(self):
        """Return read json log"""
        log_entries = []
        with open(self.workdir / "tq_log.json", "r") as f:
            for line in f:
                log_entries.append(json.loads(line))
        return log_entries

    def get_log(
        self,
        fields=["asctime", "levelname", "message"],
        search=None,
        match=None,
        print_log=True,
    ):
        """Print log entries"""
        log = self.read_log()
        filtered = filter_res(
            log, fields=fields, search=search, match=match, print_res=print_log
        )
        return filtered

    def summary_by_task(
        self,
        fields=["task_id", "running_time", "cores", "command"],
        search=None,
        match=r".",
        all_fields=False,
        print_res=True,
        fname=None,
    ):
        """Summarize queue stats by task"""
        avail_fields = [
            "task_id",
            "command",
            "cores",
            "pre",
            "post",
            "cdir",
            "workdir",
            "execfile",
            "logfile",
            "errfile",
            "slots",
            "start_ts",
            "end_ts",
            "running_time",
            "err_msg",
        ]
        bad_fields = [f for f in fields if f not in avail_fields]
        if len(bad_fields) > 0:
            msg = f"Invalid fields {bad_fields}. Avialable {avail_fields}"
            raise ValueError(msg)
        fields = avail_fields if all_fields else fields

        # Build dictionary of task attributes according to fields list
        get_info = lambda x: [(f, getattr(x, f)) for f in fields]

        task_info = []
        for task in self.running:
            task_info.append(dict([("status", "running")] + get_info(task)))
        for task in self.queue:
            task_info.append(dict([("status", "queued")] + get_info(task)))
        for task in self.completed:
            task_info.append(dict([("status", "completed")] + get_info(task)))
        for task in self.errored:
            task_info.append(dict([("status", "errored")] + get_info(task)))
        for task in self.timed_out:
            task_info.append(dict([("status", "timed_out")] + get_info(task)))
        for task in self.invalid:
            task_info.append(dict([("status", "invalid")] + get_info(task)))

        fields = ["status"] + fields
        filtered = filter_res(
            task_info,
            fields=fields,
            search=search,
            match=match,
            print_res=print_res,
            output_file=fname,
        )

        return filtered

    def summary_by_slot(
        self,
        fields=[
            "idx",
            "host",
            "status",
            "num_tasks",
            "task_ids",
            "free_time",
            "busy_time",
        ],
        search=None,
        match=r".",
        all_fields=False,
        print_res=True,
        fname=None,
    ):
        """Summarize queue stats by slots"""
        avail_fields = [
            "idx",
            "host",
            "status",
            "num_tasks",
            "task_ids",
            "free_time",
            "busy_time",
        ]
        bad_fields = [f for f in fields if f not in avail_fields]
        if len(bad_fields) > 0:
            msg = f"Invalid fields {bad_fields}. Avialable {avail_fields}"
            raise ValueError(msg)

        slot_info = []
        for s in self.task_slots:
            slot_info.append(
                {
                    "idx": s.idx,
                    "host": s.host,
                    "status": "FREE" if s.free else "BUSY",
                    "num_tasks": len(s.tasks),
                    "task_ids": [t.task_id for t in s.tasks],
                    "free_time": s.free_time,
                    "busy_time": s.busy_time,
                }
            )

        fields = fields
        filtered = filter_res(
            slot_info,
            fields=fields,
            search=search,
            match=match,
            print_res=print_res,
            output_file=fname,
        )

        return filtered

    def cleanup(self):
        """Clean-up Task Queue by removing workdir"""
        shutil.rmtree(str(self.workdir))
