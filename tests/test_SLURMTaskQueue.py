"""
SLURMTaskQueue Tests

Mocks SLURM executing environment by setting os env variables before
initializing test task queues. `ibrun` is aliased to `echo` within each task so
that main parallel command does not fail (when we don't want it to).

TODO:
    - Figure out and make sure the following directories are not left after tests:
        .stq-job123456-8uhc7ekv/
        tests/test_tq/
    - Check functional output/logs of excuting queues. Coverage there as of now
    but need to check logic.
"""
import json
import time
import os
import pdb
import random
import shutil
import sys
from pathlib import Path
from unittest.mock import patch
from conftest import *
from pyslurmtq.SLURMTaskQueue import SLURMTaskQueue

import pytest

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

class TestSLURMTaskQueue:
    def test_default_workdir(self, single_node_single_task, test_dir):
        """Test using default workdir"""
        task_file = gen_test_file(test_dir)
        tq = SLURMTaskQueue(task_file=task_file, workdir=def_test_dir)
        assert str(tq.workdir.resolve()) == str(def_test_dir)
        tq.cleanup()

    def test_bad_task(self, single_bad_task_queue):
        """Test using default workdir"""
        assert len(single_bad_task_queue.queue) == 0
        log = single_bad_task_queue.get_log(search='message', match=r'Bad task')
        assert len(log) == 1
        msg =  'Bad task in list at idx 0: Cores for task must be >=0'
        assert log[0]['message'] == msg
        single_bad_task_queue.run() # should do nothing

    def test_single_task(self, single_task_queue):
        """Sinle task in queue that fits slots and succeeds"""
        assert len(single_task_queue.queue) == 1
        single_task_queue.run()
        assert len(single_task_queue.completed) == 1
        log = single_task_queue.get_log(search='message', match='DONE')
        assert len(log) == 1
        assert single_task_queue.completed[0].running_time > 1.0
        assert single_task_queue.completed[0].running_time < 2.0

    def test_failed_task(self, single_task_error_queue):
        """Sinle task in queue that fits slots and succeeds"""
        assert len(single_task_error_queue.queue) == 1
        single_task_error_queue.run()
        assert len(single_task_error_queue.errored) == 1
        task = single_task_error_queue.errored[0]
        assert 'ibrun: command not found' in task.err_msg


    def test_single_pre_post_cdir(self, single_pre_post_task_queue):
        """Task with pre/post process steps, and cdir option"""
        assert len(single_pre_post_task_queue.queue) == 1
        single_pre_post_task_queue.run()
        assert len(single_pre_post_task_queue.completed) == 1
        log = single_pre_post_task_queue.get_log(search='message', match='DONE')
        assert len(log) == 1

    def test_too_large(self, single_too_large_task_queue):
        """Single task that is too large for queue"""
        assert len(single_too_large_task_queue.queue) == 1
        single_too_large_task_queue.run()
        assert len(single_too_large_task_queue.invalid) == 1
        task = single_too_large_task_queue.invalid[0]
        assert 'Invalid task' in task.err_msg

    def test_queue_timeout(self, timeout_task_queue):
        """Single task that is too large for queue"""
        assert len(timeout_task_queue.queue) == 1
        timeout_task_queue.run()
        assert len(timeout_task_queue.timed_out) == 1
        task = timeout_task_queue.timed_out[0]
        assert 'Exceeded max runtime' in task.err_msg

    def test_task_timeout(self, task_timeout_task_queue):
        """Single task that is too large for queue"""
        assert len(task_timeout_task_queue.queue) == 1
        task_timeout_task_queue.run()
        assert len(task_timeout_task_queue.timed_out) == 1
        task = task_timeout_task_queue.timed_out[0]
        assert 'Task 0 has exceeded task max runtime' in task.err_msg

    def test_multiple_random(self, multiple_random_task_queue):
        """Multiple tasks with random number of cores, all succeed"""
        summary = multiple_random_task_queue.summary_by_task()
        assert len(summary) == 10
        multiple_random_task_queue.run()
        summary = multiple_random_task_queue.summary_by_task(
                fields=['task_id', 'running_time', 'cores', 'command',
           'pre', 'post', 'err_msg'])
        statuses = list(set([x['status'] for x in summary]))
        assert len(statuses) == 1
        assert statuses[0] == 'completed'

    def test_multiple_good_and_bad(self, multiple_good_and_bad_queue):
        """Ranomd tasks, bad tasks, and some that will timeout"""
        summary = multiple_good_and_bad_queue.summary_by_task()
        assert len(summary) == 13
        multiple_good_and_bad_queue.run()
        summary = multiple_good_and_bad_queue.summary_by_task(
                fields=['task_id', 'running_time', 'cores', 'command',
                    'pre', 'post', 'err_msg'])
        completed = [x for x in summary if x['status'] == 'completed']
        assert len(completed) == 5
        errored = [x for x in summary if x['status'] == 'errored']
        assert len(errored) == 2
        invalid = [x for x in summary if x['status'] == 'invalid']
        assert len(invalid) == 3

        slot_summary =  multiple_good_and_bad_queue.summary_by_slot()
        assert all([x['free_time'] < 2.0 for x in slot_summary])
        assert all([x['busy_time'] > 2.0 for x in slot_summary])
