"""
Tests for Task class.

"""
import pdb
import time
import pytest
from pathlib import Path

from conftest import test_dir
from pyslurmtq.Task import Task

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

class TestTask:
    """Unit tests for the task class"""
    def test_failed_task(self, test_dir):
        """Task that will fail because ibrun not defined"""
        task = Task(0, 'echo test', test_dir)
        task.execute(0, 1)
        time.sleep(0.5)
        rc = task.get_rc()
        assert rc != 0
        assert 'ibrun: command not found' in task.err_msg
        error = task.read_err()
        assert 'ibrun: command not found' in error[0]
        log = task.read_log()
        assert len(log) == 0

    def test_successful_task(self, test_dir):
        """Task that succeeds because ibrun is aliased in pre process"""
        pre = 'alias ibrun="echo"; shopt -s expand_aliases'
        post = 'echo DONE > DONE'
        cdir = f'{test_dir}'
        task = Task(1, 'dummy-command', test_dir, pre=pre, post=post, cdir=cdir)
        task.execute(1, 2)
        time.sleep(0.5)
        rc = task.get_rc()
        assert rc == 0

    def test_terminate_task(self, test_dir):
        """Test terminating a task. Long pre process step."""
        task = Task(2, 'echo test', test_dir, pre='sleep 20')
        task.terminate() # should do nothing
        task.execute(3, 2)
        rc = task.get_rc()
        assert rc is None
        task.terminate()
        time.sleep(0.5)
        rc = task.get_rc()
        assert rc != 0

    def test_bad_task(self, test_dir):
        """Basic tests for the task class"""
        with pytest.raises(ValueError):
            task = Task(0, 'echo test', test_dir, cores=-1)

    def test_task_file(self, test_dir):
        """Task that succeeds because ibrun is aliased in pre process"""
        pre = 'alias ibrun="echo"; shopt -s expand_aliases'
        post = 'echo DONE > DONE'
        cdir = f'{test_dir}'
        task = Task(1, 'dummy-command', test_dir, pre=pre, post=post, cdir=cdir)

        with pytest.raises(ValueError):
            done_msg = task.read_task_file('DONE', lineno=1)[0]

        task.execute(1, 2)
        time.sleep(0.5)

        assert Path(f'{test_dir}/DONE').exists()
        done_msg = task.read_task_file('DONE', lineno=1)[0]
        assert done_msg == 'DONE\n'

    def test_task_read_log(self, test_dir):
        """Task that succeeds because ibrun is aliased in pre process"""
        pre = 'pwd'
        task = Task(1, 'dummy-command', test_dir, pre=pre)
        log = task.read_log()
        assert log == None
        task.execute(0, 1)
        time.sleep(0.5)
        log = task.read_log()
        assert log[0] == f'{test_dir}\n'
        log = task.read_log(lineno=1)
        assert log[0] == f'{test_dir}\n'
        log = task.read_log(lineno=[1, 2])
        assert log[0] == f'{test_dir}\n'
        assert log[1] == ''

    def test_task_read_error(self, test_dir):
        """Task that succeeds because ibrun is aliased in pre process"""
        pre = 'pwd'
        task = Task(1, 'dummy-command', test_dir, pre=pre)
        err = task.read_err()
        assert err == None
        task.execute(0, 1)
        time.sleep(0.5)
        err = task.read_err()
        assert 'ibrun: command not found' in err[0]
        err = task.read_err(lineno=1)
        assert 'ibrun: command not found' in err[0]
        err = task.read_err(lineno=[1, 2])
        assert 'ibrun: command not found' in err[0]
        assert err[1] == ''

    def test_str_rep(self, test_dir):
        """Test how tasks are outputed to strings"""
        task = Task(1, 'echo', test_dir, pre='pwd')
        s = str(task)
        assert 'pre:<<pwd>>' in s
        assert 'cmnd:<<echo>>' in s
        assert 'cores:1' in s
