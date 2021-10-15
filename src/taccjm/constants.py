"""
TACCJM Constants

"""

import os

# Location on disk where to find taccjm source code
TACCJM_SOURCE = os.path.dirname(__file__)

# Dir where to store taccjm logs and other files
TACCJM_DIR = os.path.join(os.path.expanduser("~"),'.taccjm')

# Default host and port to start taccjm servers on
TACCJM_PORT = 8221
TACCJM_HOST = 'localhost'

# Basic HPC Application Template

# Example Project Config (.ini) file. Values to be added into app/job configs
CONFIG_TEMPLATE = """[app]
# These values will be templated into app json file.
name = template-app
version = 0.0.0
short_desc = Template Application
long_description = Template to build HPC apps.

[job]
# Can also specify job parameters here
desc = A test run of the template hpc application

# Note job queue may change depending on what system application is on
queue = development

# Test input file from local system to send to job
job_input = test_file.txt

# Test job parameter to be passed to job run
job_parameter = 1
"""

# Example APP and JOB configs, with jinja template string patterns
APP_TEMPLATE = {'name': '{{ app.name }}--{{ app.version }}',
                'short_desc': '{{ app.short_desc }}',
                'long_desc':  '{{ app.long_desc }}',
                'default_node_count': 1,
                'default_processors_per_node': 10,
                'default_memory_per_node': '1',
                'default_max_run_time': '00:10:00',
                'entry_script': 'run.sh',
                'inputs': [{'name': 'input1',
                            'label': 'Input argument',
                            'desc': 'Input to be copied to job dir.'}],
                'parameters': [{'name': 'param1',
                                'label': 'Parameter argument',
                                'desc': 'value to be parsed into run script'}],
                'outputs': [{'name': 'output1',
                             'label': 'Output',
                             'desc': 'Output produced by application.'}]}
JOB_TEMPLATE = {'name': '{{ app.name }}-job-test',
                'app': '{{ app.name }}--{{ app.version}}',
                'desc': '{{ job.desc }}',
                'queue': '{{ job.queue }}',
                'node_count': 1,
                'processors_per_node': 2,
                'memory_per_node': '1',
                'max_run_time': '00:01:00',
                'inputs': {'input1': '{{ job.input }}'},
                'parameters': {'param1': '{{ job.parameter }}'}}

# Example of an application entry point script.
APP_SCRIPT_TEMPLATE = """#### BEGIN SCRIPT LOGIC
head ${input1} -n ${param1} >out.txt 2>&1"""

# SLURM Config Template
SUBMIT_SCRIPT_TEMPLATE = """#!/bin/bash
#----------------------------------------------------
# {name}
# {desc}
#----------------------------------------------------

#SBATCH -J {job_id}                               # Job name
#SBATCH -o {job_id}.o%j                           # Name of stdout output file
#SBATCH -e {job_id}.e%j                           # Name of stderr error file
#SBATCH -p {queue}                                # Queue (partition) name
#SBATCH -N {node_count}                           # Total # of nodes
#SBATCH -n {node_count*processors_per_node}       # Total # of mpi tasks
#SBATCH -t {max_run_time}                         # Run time (hh:mm:ss)"""


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

