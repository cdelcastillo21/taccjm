"""
Conftest for taccjm library

Defines utility functions and fixtures for executing unit tests for all
classes in the library. SLURM executing environment are mocked in fixtures by
setting/unsetting os env variables before/after tests. Note furthermore how
`ibrun` is aliased to `echo` within each task so that main parallel command does
not fail (when we don't want it to).
"""


"""
    Dummy conftest.py for taccjm.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""

import random
import os
import pdb
import json
import pytest
from pathlib import Path
import shutil
import pytest
import shutil
from pathlib import Path
from unittest.mock import patch
from taccjm import TACCSSHClient
from paramiko import SSHException
from taccjm.pyslurmtq.SLURMTaskQueue import SLURMTaskQueue

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def_test_dir = Path(__file__).parent / ".test_dir"


def pytest_addoption(parser):
    parser.addoption("--mfa", action="store",
                     default="012345", help="MFA token. Must be provided")


@pytest.fixture
def mfa(request):
    return request.config.getoption("--mfa")


@pytest.fixture()
def test_dir():
    if def_test_dir.exists():
        shutil.rmtree(def_test_dir)
    def_test_dir.mkdir(exist_ok=True)
    test_dir_path = Path(def_test_dir).absolute()
    yield test_dir_path
    try:
        shutil.rmtree(test_dir_path)
    except:
        pass


@pytest.fixture()
def test_file(test_dir):
    file_path = f'{test_dir}/hello.txt'
    with open(file_path, 'w') as f:
        f.write('Hello World!')
    yield file_path


@pytest.fixture
@patch.object(TACCSSHClient, 'connect')
@patch.object(TACCSSHClient, 'execute_command')
def mocked_client(connect, execute_command):
    client = TACCSSHClient('stampede2', user='test', psw='test', mfa=123456)
    return client


# Command succeeds, just mock the exec_command function in
class good_channel():

    def __init__(self, active=False):
        self.active = active
        pass

    def exec_command(self, cmd):
        pass

    def exit_status_ready(self):
        return True if not self.active else False

    def recv_exit_status(self):
        return 0

    def recv(self, nbytes):
        out = b'test'
        return out[0:nbytes]

    def recv_stderr(self, nbytes):
        return b''

    def close(self):
        pass


class bad_channel(good_channel):

    def recv_stderr(self, nbytes):
        err = b'error'
        return err[0:nbytes]

    def recv_exit_status(self):
        return 1


class good_transport():

    def __init__(self):
        pass

    def open_session(self):
        channel = good_channel()
        return channel


class bad_transport():

    def __init__(self):
        pass

    def open_session(self):
        raise SSHException


def gen_test_file(
        test_dir,
        cmnd="echo main",
        cores=1,
        num_tasks=1,
        sleep=1,
        pre=None,
        post=None,
        cdir=None,
        bad_tasks=0,
        fail_tasks=0,
        random_tasks=0,
        max_cores=4,
        max_sleep=1,
        seed=21,
):
    if pre is not None:
        pre = f"sleep {sleep}; {pre}"
    if post is not None:
        post = f"sleep {sleep}; {post}"
    cdir = f"{def_test_dir}" if cdir else None

    task_file = test_dir / ".test_task_file.json"

    tasks = []
    if num_tasks > 0:
        tasks += [{"cmnd": cmnd, "cores": cores,
                   "pre": pre, "post": post,
                   'cdir': cdir} for x in range(num_tasks)]

    if bad_tasks > 0:
        tasks += [{"cmnd": "echo bad", "cores": -1} for x in range(bad_tasks)]
    if fail_tasks > 0:
        tasks += [{"cmnd": 'echo', "cores": cores,
                   "pre": pre, "post": post,
                   'cdir': cdir} for x in range(fail_tasks)]
    if random_tasks > 0:
        random.seed(seed)
        tasks += [{
            "cmnd": cmnd,
            "cores": random.randint(1, max_cores),
            "pre": f"sleep {random.uniform(0.1,max_sleep)}; {pre}",
            "post": post,
            "cdir": cdir,
        }
                  for x in range(random_tasks)
                  ]
    with open(str(task_file), "w") as fp:
        json.dump(tasks, fp)

    return str(task_file)


@pytest.fixture
def single_node_single_task():
    """Single host with a single allocated task slots"""

    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c001-001"
    os.environ["SLURM_TASKS_PER_NODE"] = "1"
    yield None
    os.environ["SLURM_JOB_ID"] = ""
    os.environ["SLURM_JOB_NODELIST"] = ""
    os.environ["SLURM_TASKS_PER_NODE"] = ""


@pytest.fixture
def single_node_multiple_tasks():
    """Single host with multiple allocated task slots"""

    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c001-001"
    os.environ["SLURM_TASKS_PER_NODE"] = "10"
    yield None
    os.environ["SLURM_JOB_ID"] = ""
    os.environ["SLURM_JOB_NODELIST"] = ""
    os.environ["SLURM_TASKS_PER_NODE"] = ""


@pytest.fixture
def multiple_node_multiple_tasks():
    """Multiple hosts, with multiple tasks allowed per host (11 total)"""
    os.environ["SLURM_JOB_ID"] = "123456"
    os.environ["SLURM_JOB_NODELIST"] = "c303-[005-007,011],c304-005"
    os.environ["SLURM_TASKS_PER_NODE"] = "2(x4),3"
    yield None
    os.environ["SLURM_JOB_ID"] = ""
    os.environ["SLURM_JOB_NODELIST"] = ""
    os.environ["SLURM_TASKS_PER_NODE"] = ""


@pytest.fixture
def single_bad_task_queue(single_node_single_task, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="echo main",
            cores=1,
            num_tasks=0,
            bad_tasks=1)
    tq = SLURMTaskQueue(task_file=task_file)
    yield tq
    tq.cleanup()


@pytest.fixture
def single_task_queue(single_node_single_task, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="echo main ; echo NOERROR",
            cores=1,
            num_tasks=1,
            bad_tasks=0)
    tq = SLURMTaskQueue(task_file=task_file, workdir=def_test_dir)
    yield tq
    tq.cleanup()


@pytest.fixture
def single_task_error_queue(single_node_single_task, test_dir):

    task_file = gen_test_file(
            test_dir,
            cores=1,
            num_tasks=0,
            fail_tasks=1,
            bad_tasks=0)
    tq = SLURMTaskQueue(task_file=task_file, workdir=def_test_dir)
    yield tq
    tq.cleanup()


@pytest.fixture
def single_pre_post_task_queue(single_node_single_task, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="touch foo",
            cores=1,
            pre='alias ibrun="echo"; shopt -s expand_aliases',
            post='echo $(pwd)',
            cdir=True,
            num_tasks=1,
            sleep=0.1)
    tq = SLURMTaskQueue(task_file=task_file)
    yield tq
    tq.cleanup()


@pytest.fixture
def single_too_large_task_queue(single_node_single_task, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="echo",
            cores=10,
            num_tasks=1,
            sleep=0.1)
    tq = SLURMTaskQueue(task_file=task_file)
    yield tq
    tq.cleanup()


@pytest.fixture
def timeout_task_queue(single_node_single_task, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="echo",
            cores=1,
            num_tasks=1,
            pre="echo pre",
            sleep=10)
    tq = SLURMTaskQueue(task_file=task_file, max_runtime=0.1)
    yield tq
    tq.cleanup()


@pytest.fixture
def task_timeout_task_queue(single_node_single_task, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="echo",
            cores=1,
            num_tasks=1,
            pre="echo pre",
            sleep=10)
    tq = SLURMTaskQueue(task_file=task_file, task_max_runtime=0.1)
    yield tq
    tq.cleanup()


@pytest.fixture
def multiple_task_queue(single_node_multiple_tasks, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="echo",
            cores=1,
            num_tasks=10,
            sleep=0.1)
    tq = SLURMTaskQueue(task_file=task_file)
    yield tq
    tq.cleanup()


@pytest.fixture
def multiple_random_task_queue(multiple_node_multiple_tasks, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="touch foo",
            pre='alias ibrun="echo"; shopt -s expand_aliases',
            post='echo $(pwd)',
            num_tasks=0,
            random_tasks=10,
            max_cores=7,
            sleep=0.01,
            max_sleep=0.1)
    tq = SLURMTaskQueue(task_file=task_file, delay=0.1)
    yield tq
    tq.cleanup()


@pytest.fixture
def multiple_good_and_bad_queue(multiple_node_multiple_tasks, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="touch foo; echo NOERROR",
            pre='echo PRE',
            post='echo POST',
            random_tasks=10,
            bad_tasks=1,
            fail_tasks=2,
            max_cores=15,
            sleep=0.1,
            max_sleep=2)
    tq = SLURMTaskQueue(task_file=task_file, task_max_runtime=1, delay=0.1)
    yield tq
    tq.cleanup()

