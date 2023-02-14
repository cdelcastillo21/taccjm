"""
Test Suite

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
from conftest import test_dir, multiple_node_multiple_tasks, gen_test_file

import pytest
from pyslurmtq.pyslurmtq import main, run

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def test_run(capsys, multiple_node_multiple_tasks, test_dir):

    task_file = gen_test_file(
            test_dir,
            cmnd="touch foo; echo NOERROR",
            pre='echo PRE',
            post='echo POST',
            random_tasks=10,
            bad_tasks=1,
            fail_tasks=2,
            max_cores=15,
            sleep = 0.1,
            max_sleep=2)
    testargs = [
        "pyslurmtq",
        f"{task_file}",
        "--workdir",
        f"{test_dir}",
        "--delay",
        "1",
        "--task-max-rt",
        "5",
        "--max-rt",
        "10",
        "--no-cleanup",
        "-v",
    ]
    with patch.object(sys, "argv", testargs):
        run()

    # TODO: Check output and delte default workdir created by task queue
