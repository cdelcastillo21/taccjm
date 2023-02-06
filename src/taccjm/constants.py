"""
TACCJM Constants

"""

import os

# Location on disk where to find taccjm source code
TACCJM_SOURCE = os.path.dirname(__file__)

# Dir where to store taccjm logs and other files
TACCJM_DIR = os.path.join(os.path.expanduser("~"), ".taccjm")

# Default host and port to start tacc ssh servers on
TACC_SSH_PORT = 8221
TACC_SSH_HOST = "localhost"

# Basic HPC Application Template
# Example APP and JOB configs
APP_TEMPLATE = {
    "name": "template-app",
    "short_desc": "Template Application",
    "long_desc": "Template to build HPC Apps",
    "default_node_count": 1,
    "default_processors_per_node": 10,
    "default_max_run_time": "00:10:00",
    "default_queue": "development",
    "entry_script": "run.sh",
    "inputs": [
        {
            "name": "input1",
            "label": "Input argument",
            "desc": "Input to be copied to job dir.",
        }
    ],
    "parameters": [
        {
            "name": "param1",
            "label": "Parameter argument",
            "desc": "value to be parsed into run script",
        }
    ],
}
JOB_TEMPLATE = {
    "name": "template-app-job-test",
    "app": "template-app",
    "desc": "A test run of the tempalte hpc application.",
    "queue": "development",
    "node_count": 1,
    "processors_per_node": 2,
    "memory_per_node": "1",
    "max_run_time": "00:01:00",
    "inputs": {"input1": "input.txt"},
    "parameters": {"param1": "1"},
}

# Example of an application entry point script.
APP_SCRIPT_TEMPLATE = """#### BEGIN SCRIPT LOGIC
sleep 30
head ${input1} -n ${param1} >out.txt 2>&1"""

# SLURM Config Template
SUBMIT_SCRIPT_TEMPLATE = """#!/bin/bash
#----------------------------------------------------
# {name}
# {desc}
#----------------------------------------------------

#SBATCH -J {job_id}     # Job name
#SBATCH -o {job_id}.o%j # Name of stdout output file
#SBATCH -e {job_id}.e%j # Name of stderr error file
#SBATCH -p {queue}      # Queue (partition) name
#SBATCH -N {N}          # Total num nodes
#SBATCH -n {n}          # Total num mpi tasks
#SBATCH -t {rt}         # Run time (hh:mm:ss)"""


def make_taccjm_dir():
    """
    Make TACCJM Directories

    Make local directory, at ~/.taccjm, to store taccjm files, if it needs to
    be created.

    Parameters
    ----------

    Returns
    -------

    """
    if not os.path.exists(TACCJM_DIR):
        os.makedirs(TACCJM_DIR)
