"""
TACCJM Hug Server

Manages TACCJM instances offers access points via http endpoints using the hug library.


"""
import pdb
import os
import hug
import falcon
import logging
from taccjm.TACCJobManager import TACCJobManager, TJMCommandError


__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


# For storing logs and state
TJM_DIR = os.environ.get("TACCJM_DIR")
TJM_DIR = "~/.taccjm" if TJM_DIR is None else TJM_DIR
if not os.path.exists(TJM_DIR):
    os.makedirs(TJM_DIR)

# Initialize server
logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)

# Dictionary containing all job manager instances being managed
# Note there could be multiple instance if managing more than one system
JM = {}

# @hug.exception(AssertionError)
# def handle_custom_exceptions(exception):
#     # Handles Assertion Errors - Usually for invalid inputs
#     msg = exception.args[0]
#     return {'error': msg}


def _check_init(jm_id):
    """Check if Job Manager is initalized"""
    if jm_id not in JM.keys():
        raise falcon.HTTPError(falcon.HTTP_404, "jm_error", f"TACCJM {jm_id} does not exist.")

@hug.post('/init')
def init_jm(jm_id:str, system:str, user:str, psw:str, mfa:str):
    """Initialize Job Manager Instances"""
    global JM

    if jm_id not in JM.keys():
        try:
            JM[jm_id] = TACCJobManager(system, user=user, psw=psw, mfa=mfa)
            logger.info(f"SUCCESS - TACCJM {jm_id} initialized successfully.")

            ret = {'jm_id':jm_id, 'sys':JM[jm_id].system, 'user':JM[jm_id].user,
                   'apps_dir':JM[jm_id].apps_dir, 'jobs_dir':JM[jm_id].jobs_dir}
        except ValueError as v:
            # Raise Not FOund HTTP code for non TACC system
            raise falcon.HTTPError(falcon.HTTP_404, "jm_error",
                    f"Unable to initialize TACCJM {jm_id} on system {system} for user {user}: {v}")
        except Exception as e:
            # Raise Unauthorized HTTP code for bad login to system
            raise falcon.HTTPError(falcon.HTTP_401, "jm_error",
                    f"Unable to initialize TACCJM {jm_id} on system {system} for user {user}: {e}")

        return ret
    else:
        # Raise Conflict HTTP error
        raise falcon.HTTPError(falcon.HTTP_409, "jm_error", f"TACCJM {jm_id} already exists.")


@hug.get('/list')
def list_jm():
    """Show initialized job managers"""
    # out = f"{'JM_ID'.rjust(10)|{'SYS'.rjust(20)}|{'USER'.rjust(10)}\n"
    out = []
    for jm in JM.keys():
        out.append({'jm_id':jm, 'sys':JM[jm].system, 'user':JM[jm].user,
            'apps_dir':JM[jm].apps_dir, 'jobs_dir':JM[jm].jobs_dir})
    return out


@hug.get('/{jm_id}')
def get_jm(jm_id:str):
    """Get Job Manager managed by this server if it exists"""

    _check_init(jm_id)

    jm = {'jm_id':jm_id, 'sys':JM[jm_id].system, 'user':JM[jm_id].user,
          'apps_dir':JM[jm_id].apps_dir, 'jobs_dir':JM[jm_id].jobs_dir}
    return jm


@hug.get('/{jm_id}/queue')
def get_queue(jm_id:str, user:str=None):
    """Show job queue for user on system."""

    _check_init(jm_id)

    return JM[jm_id].showq(user=user)


@hug.get('/{jm_id}/allocations')
def allocations(jm_id:str):
    """List all allocatiosn for user on system."""

    _check_init(jm_id)

    return JM[jm_id].get_allocations()


@hug.get('/{jm_id}/files/list')
def list_files(jm_id:str, path:str="~"):
    """List files on system"""
    _check_init(jm_id)

    try:
        files = JM[jm_id].list_files(path=path)
    except TJMCommandError as e:
        # Raise 404 not found error if can't list files at given path
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(e))

    return files

@hug.get('/{jm_id}/files/peak')
def peak_file(jm_id:str, path:str, head:int=-1, tail:int=-1):
    """Peak File

    Extract first or last lines of a file via head/tail command.
    """
    _check_init(jm_id)

    try:
        JM[jm_id].peak_file(self, head=head, tail=tail)
    except FileNotFoundError as f:
        # Raise 404 not found error if local or remoate path don't exist
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to access either paath
        raise falcon.HTTPError(falcon.HTTP_403, "files", str(p))


@hug.put('/{jm_id}/files/upload')
def upload(jm_id:str, local_path:str, remote_path:str, file_filter:str='*'):
    """File Upload

    Upload file or folder to TACC system for given job manager

    """
    _check_init(jm_id)

    try:
        JM[jm_id].upload(local_path, remote_path, file_filter=file_filter)
    except FileNotFoundError as f:
        # Raise 404 not found error if local or remoate path don't exist
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to access either paath
        raise falcon.HTTPError(falcon.HTTP_403, "files", str(p))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "files", str(e))


@hug.get('/{jm_id}/files/download')
def download(jm_id:str, remote_path:str, local_path:str, file_filter:str='*'):
    """File Download

    Download file or folder to TACC system for given job manager to local path

    """
    _check_init(jm_id)

    try:
        JM[jm_id].download(remote_path, local_path, file_filter='*')
    except FileNotFoundError as f:
        # Raise 404 not found error if local or remoate path don't exist
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to access either paath
        raise falcon.HTTPError(falcon.HTTP_403, "files", str(p))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "files", str(e))


@hug.delete('/{jm_id}/files/remove')
def remove(jm_id:str, remote_path:str):
    """Remove file. In reality just moves file to JM's trash directory.

    """
    _check_init(jm_id)

    try:
        JM[jm_id].remove(remote_path)
    except FileNotFoundError as f:
        # Raise 404 not found error remote_path to remove does not exist
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to access remote_path
        raise falcon.HTTPError(falcon.HTTP_403, "files", str(p))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "files", str(e))


@hug.put('/{jm_id}/files/restore')
def restore(jm_id:str, remote_path:str):
    """Restore file. Restore previously removed file in trash directory to original location.

    """
    _check_init(jm_id)

    try:
        JM[jm_id].restore(remote_path)
    except FileNotFoundError as f:
        # Raise 404 not found error remote_path to remove does not exist
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(f))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "files", str(e))


@hug.put('/{jm_id}/data/send')
def send_data(jm_id:str, data, remote_path:str):
    """Send data

    Send text or json data directly to a path on JM's TACC system.

    """
    _check_init(jm_id)

    try:
        JM[jm_id].send_data(data, remote_path)
    except ValueError as v:
        # Raise 400 Bad Request if invalid data type passed
        raise falcon.HTTPError(falcon.HTTP_400, "data", str(v))
    except FileNotFoundError as f:
        # Raise 404 if remote_path does not exist
        raise falcon.HTTPError(falcon.HTTP_404, "data", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to remote_path
        raise falcon.HTTPError(falcon.HTTP_403, "data", str(p))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "data", str(e))


@hug.get('/{jm_id}/data/receive')
def receive_data(jm_id:str, remote_path:str, data_type:str='text'):
    """File Download

    Download file or folder to TACC system for given job manager to local path

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].get_data(remote_path, data_type=data_type)
    except ValueError as v:
        # Raise bad request if data_type is not text or json
        raise falcon.HTTPError(falcon.HTTP_400, "data", str(v))
    except FileNotFoundError as f:
        # Raise 404 not found error if local or remoate path don't exist
        raise falcon.HTTPError(falcon.HTTP_404, "data", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to access either paath
        raise falcon.HTTPError(falcon.HTTP_403, "data", str(p))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "data", str(e))


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
