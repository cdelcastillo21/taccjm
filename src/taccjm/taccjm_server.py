"""
TACCJM Hug Server

Manages TACCJM instances offers access points via http endpoints using the hug library.


"""
import pdb
import os
import hug
import pickle
import logging
import subprocess
from TACCJobManager import TACCJobManager

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# # TODO: Create custom exceptions for api and place these in separate file later
# class CustomException(Exception):
#     """Exception raised for JM errors .
#
#     Attributes:
#     input -- input which caused the error
#     message -- explanation of the error
#     """
#
#     def __init__(self, input, message="Error for some reason"):
#         self.input = input
#         self.message = message
#         super().__init__(self.message)
#         return -1

global TJM_DIR

# For storing logs and state
TJM_DIR = os.environ.get("TACCJM_DIR")
TJM_DIR = "~/.taccjm" if TJM_DIR is None else TJM_DIR

# Initialize server
logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)

# Dictionary containing all job manager instances being managed
# Note there could be multiple instance if managing more than one system
JM = {}


def check_init(jm_id):
    assert jm_id in JM.keys(), f"ERROR - TACC JM {jm_id} has not been initialized."


@hug.exception(AssertionError)
def handle_custom_exceptions(exception):
  # Handles Assertion Errors that occur when TACCJM is initialized already
  msg = exception.args[0]
  return {'error': msg}


@hug.post('/jm/init')
def init_jm(jm_id:str, system:str, user:str, psw:str, mfa:str):

    assert jm_id not in JM.keys(), f"ERROR - TACCJM {jm_id} initialized already."

    JM[jm_id] = TACCJobManager(system, user=user, psw=psw, mfa=mfa)

    logger.info(f"SUCCESS - TACCJM {jm_id} initialized successfully.")

    ret = {'jm_id':jm_id, 'sys':JM[jm_id].system, 'user':JM[jm_id].user,
           'app_dir':JM[jm_id].app_dir, 'jobs_dir':JM[jm_id].jobs_dir}

    return ret


@hug.get('/jm/list')
def list_jm():
    """Show initialized job managers"""
    # out = f"{'JM_ID'.rjust(10)|{'SYS'.rjust(20)}|{'USER'.rjust(10)}\n"
    out = []
    for jm in JM.keys():
        out.append({'jm_id':jm, 'sys':JM[jm].system, 'user':JM[jm].user,
            'app_dir':JM[jm].app_dir, 'jobs_dir':JM[jm].jobs_dir})
    return out


@hug.get('/jm/get/{jm_id}')
def get_jm(jm_id:str):
    """Get Job Manager managed by this server if it exists"""

    assert jm_id in JM.keys(), f"ERROR - TACCJM {jm_id} does not exist."

    jm = {'jm_id':jm_id, 'sys':JM[jm_id].system, 'user':JM[jm_id].user,
          'app_dir':JM[jm_id].app_dir, 'jobs_dir':JM[jm_id].jobs_dir}
    return jm


# @hug.get('/queue')
# def queue(jm_id:str):
#     """Show job queue for user on system."""
#     check_init(jm_id)
#     return JM[jm_id].showq()
# 
# 
# @hug.get('/allocations')
# def allocations(jm_id:str):
#     """List all allocatiosn for user on system."""
#     check_init(jm_id)
#     return JM[jm_id].showq()
# 
# 
# @hug.get('/files/list')
# def list_files(jm_id:str, path:str):
#     check_init(jm_id)
#     files = JM[jm_id].list_files(path=path)
#     return files
# 
# 
# @hug.put('/files/send')
# def send_file(jm_id:str, local_path:str, remote_path:str):
#     check_init(jm_id)
#     files = JM[jm_id].send_file(local_path, remote_path)
#     return files
# 
# 
# @hug.get('/apps')
# def apps(head:hug.types.number=-1):
#     """Gets all apps."""
#     check_init()
#     return JM.get_apps(head=head)
# 
# 
# @hug.get('/apps/load')
# def get_app_wrapper_script(app:str):
#     check_init()
#     return JM.get_app_wrapper_script(app=app)
# 
# 
# @hug.get('/jobs')
# def jobs(head:hug.types.number=-1):
#     """Gets all jobs."""
#     check_init()
#     return JM.get_jobs(head=head)
# 
# 
# @hug.get('/jobs/load')
# def load_job_config(job_id:str):
#     """Get job configuraiton."""
#     check_init()
#     return JM.load_job_config(job_id)
# 
# 
# @hug.put('/jobs/save')
# def save_job_config(job_config):
#     """Get job configuraiton."""
#     check_init()
#     return JM.save_job(job_config)
# 
# 
# @hug.post('/jobs/setup')
# def setup_job(job_config:hug.types.mapping):
#     check_init()
#     update_config = JM.setup_job(job_config=job_config)
#     return update_config['job_id']
# 
# 
# @hug.put('/jobs/submit')
# def submit_job(job_id:str):
#     check_init()
#     config = JM.load_job_config(job_id)
#     JM.submit_job(config)
# 
# 
# @hug.put('/jobs/cancel')
# def cancel_job(job_id:str):
#     check_init()
#     config = JM.load_job_config(job_id)
#     JM.cancel_job(config)
# 
# 
# @hug.delete('/jobs/cleanup')
# def cleanup_job(job_id:str):
#     check_init()
#     config = JM.load_job_config(job_id)
#     JM.cleanup_job(config, check=False)
# 
# 
# @hug.get('/jobs/ls')
# def ls_job(job_id:str, path:str=''):
#     """Query job directory"""
#     check_init()
#     config = JM.load_job_config(job_id)
#     return JM.ls_job(config, path=path)
# 
# 
# @hug.get('/jobs/file')
# def get_job_file(job_id:str, fpath:str, dest_dir, head:hug.types.number=-1, tail:hug.types.number=-1):
#     check_init()
#     config = JM.load_job_config(job_id)
#     if head>0 or tail>0:
#         return JM.peak_job_file(config, fpath, head=head, tail=tail, prnt=False)
#     else:
#         return JM.get_job_file(config, fpath, dest_dir=dest_dir)
# 
# 
# @hug.put('/jobs/file')
# def put_job_file(job_id:str, fpath:str, dest_dir:str=None):
#     check_init()
#     config = JM.load_job_config(job_id)
#     return JM.send_job_file(config, fpath, dest_dir=dest_dir)
# 
# 
# @hug.put('/apps/deploy')
# def deploy_app(app_name:str, local_app_dir:str, version=None, overwrite=False):
#     check_init()
#     return JM.deploy_app(app_name, local_app_dir, version=version, overwrite=overwrite)
# 
# 
