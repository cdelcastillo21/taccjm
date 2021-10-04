"""
TACCJM Hug Server

Server for managing instances of TACCJobManager classes via http endpoints
using the hug framework.

TODO: Add more extensive logging
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

# Initialize server logger
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
            logger.info(f"INIT - Initializing TACCJM {jm_id}.")
            JM[jm_id] = TACCJobManager(system, user=user, psw=psw, mfa=mfa,
                    working_dir=jm_id)
            logger.info(f"SUCCESS - TACCJM {jm_id} initialized successfully.")

            ret = {'jm_id':jm_id, 'sys':JM[jm_id].system, 'user':JM[jm_id].user,
                   'apps_dir':JM[jm_id].apps_dir, 'jobs_dir':JM[jm_id].jobs_dir}
            return ret
        except ValueError as v:
            # Raise Not Found HTTP code for non TACC system
            msg = f"Unable to initialize {jm_id} on {system} for {user}: {v}"
            raise falcon.HTTPError(falcon.HTTP_404, "jm_error", msg)
        except Exception as e:
            # Raise Unauthorized HTTP code for bad login to system
            msg = f"Unable to initialize {jm_id} on {system} for {user}: {e}"
            raise falcon.HTTPError(falcon.HTTP_401, "jm_error", msg)
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
        return JM[jm_id].peak_file(path, head=head, tail=tail)
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


@hug.put('/{jm_id}/files/write')
def write(jm_id:str, data, remote_path:str):
    """Write file

    Write text or json data directly to a file path on JM's remote system.
    """
    _check_init(jm_id)

    try:
        JM[jm_id].send_data(data, remote_path)
    except ValueError as v:
        # Raise 400 Bad Request if invalid data type passed
        raise falcon.HTTPError(falcon.HTTP_400, "files", str(v))
    except FileNotFoundError as f:
        # Raise 404 if remote_path does not exist
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to remote_path
        raise falcon.HTTPError(falcon.HTTP_403, "files", str(p))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "files", str(e))


@hug.get('/{jm_id}/files/read')
def read(jm_id:str, remote_path:str, data_type:str='text'):
    """Read file

    Read text or json file directly from path on remote system managed by by
    job manager instance.
    """
    _check_init(jm_id)

    try:
        return JM[jm_id].get_data(remote_path, data_type=data_type)
    except ValueError as v:
        # Raise bad request if data_type is not text or json
        raise falcon.HTTPError(falcon.HTTP_400, "files", str(v))
    except FileNotFoundError as f:
        # Raise 404 not found error if local or remoate path don't exist
        raise falcon.HTTPError(falcon.HTTP_404, "files", str(f))
    except PermissionError as p:
        # Raise 403 forbidden if dont have permissions to access either paath
        raise falcon.HTTPError(falcon.HTTP_403, "files", str(p))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "files", str(e))


@hug.get('/{jm_id}/apps/list')
def list_apps(jm_id:str):
    """List Apps

    Gets all apps.
    """
    _check_init(jm_id)

    return JM[jm_id].get_apps()


@hug.get('/{jm_id}/apps/{app_id}')
def get_app(jm_id:str, app_id:str):
    """Get App

    Get configuration for a deploy HPC Application.

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].get_app(app_id)
    except ValueError as v:
        # Raise 404 not found error if couldn't find app
        raise falcon.HTTPError(falcon.HTTP_404, "apps", str(v))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "apps", str(e))


@hug.post('/{jm_id}/apps/deploy')
def deploy_app(jm_id:str, app_config:dict, local_app_dir:str='.',
        overwrite:bool=False):
    """Deploy App

    Deploy an application from local directory to TACC system

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].deploy_app(app_config=app_config,
                local_app_dir=local_app_dir, overwrite=overwrite)
    except FileNotFoundError as f:
        # Raise 404 not found error if couldn't find app config files
        raise falcon.HTTPError(falcon.HTTP_404, "apps", str(f))
    except ValueError as v:
        # Raise 400 Bad Request if app config is invalid
        raise falcon.HTTPError(falcon.HTTP_400, "apps", str(v))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "apps", str(e))


@hug.get('/{jm_id}/jobs/list')
def list_jobs(jm_id:str):
    """Gets all jobs."""
    _check_init(jm_id)

    return JM[jm_id].get_jobs()


@hug.get('/{jm_id}/jobs/{job_id}')
def get_job(jm_id:str, job_id:str):
    """Get Job

    Get job configuration for job deployed on TACC system.

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].get_job(job_id)
    except ValueError as v:
        # Raise 404 not found error if couldn't find job
        raise falcon.HTTPError(falcon.HTTP_404, "jobs", str(v))
    except Exception as e:
        # Unknown Error
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.post('/{jm_id}/jobs/deploy')
def deploy_job(jm_id:str, job_config:dict):
    """Deploy Job

    Deploy a job to TACC system.

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].setup_job(job_config=job_config)
    except ValueError as v:
        # Raise 400 if bad job config
        raise falcon.HTTPError(falcon.HTTP_400, "jobs", str(v))
    except Exception as e:
        # Raise Internal Server error if error staging job.
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.put('/{jm_id}/jobs/{job_id}/submit')
def submit_job(jm_id:str, job_id:str):
    """Submit job

    Submit a job to the Slurm Job Queue on given TACC system

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].submit_job(job_id)
    except ValueError as v:
        # Raise 400 if job can't be submitted because it dne or not setup
        raise falcon.HTTPError(falcon.HTTP_400, "jobs", str(v))
    except Exception as e:
        # Raise Internal Server error if another error submitting job.
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))

@hug.put('/{jm_id}/jobs/{job_id}/cancel')
def cancel_job(jm_id:str, job_id:str):
    """Cancel Job

    Cancels a job that has been submitted to the SLURM job queue on given
    TACC system

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].cancel_job(job_id)
    except ValueError as v:
        # Raise 400 if job can't be cancelled because it dne or not running
        raise falcon.HTTPError(falcon.HTTP_400, "jobs", str(v))
    except Exception as e:
        # Raise Internal Server error if another error cancelling job.
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.delete('/{jm_id}/jobs/{job_id}/remove')
def cleanup_job(jm_id:str, job_id:str):
    """Cleanup Job

    Removes job directory (Sends it to trash) on given TACC system.

    """

    return JM[jm_id].cleanup_job(job_id)


@hug.get('/{jm_id}/jobs/{job_id}/files/list')
def list_job_files(jm_id:str, job_id:str, path:str=''):
    """List Job files

    List files in a job directory.

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].ls_job(job_id, path=path)
    except TJMCommandError as e:
        # Raise 404 not found error if can't list files at given path
        raise falcon.HTTPError(falcon.HTTP_404, "jobs", str(e))


@hug.get('/{jm_id}/jobs/{job_id}/files/download')
def download_job_file(jm_id:str, job_id:str, path:str, dest_dir:str='.'):
    """Download Job file/folder

    Download a file/folder from a job directory

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].download_job_data(job_id, path, dest_dir=dest_dir)
    except FileNotFoundError as f:
        # Raise 404 if path does not exist in job directory
        raise falcon.HTTPError(falcon.HTTP_404, "jobs", str(f))
    except Exception as e:
        # Raise 500 if unable to download data
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.get('/{jm_id}/jobs/{job_id}/files/read')
def read_job_file(jm_id:str, job_id:str, path:str, data_type:str='text'):
    """Read Job file

    Read a job text or json file and return contents directly.

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].get_job_data(job_id, path, data_type=data_type)
    except FileNotFoundError as f:
        # Raise 404 if path does not exist in job directory
        raise falcon.HTTPError(falcon.HTTP_404, "jobs", str(f))
    except Exception as e:
        # Raise 500 if unable to read file
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.put('/{jm_id}/jobs/{job_id}/files/upload')
def upload_job_file(jm_id:str, job_id:str, path:str, dest_dir:str='.',
        file_filter:str='*'):
    """Upload Job File/Folder

    Uplaod a file/folder to a job's directory

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].upload_job_data(job_id, path, dest_dir=dest_dir,
                file_filter=file_filter)
    except FileNotFoundError as f:
        # Raise 404 if path to upload does not exist
        raise falcon.HTTPError(falcon.HTTP_404, "jobs", str(f))
    except Exception as e:
        # Raise 500 if unable to download data
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.put('/{jm_id}/jobs/{job_id}/files/write')
def write_job_file(jm_id:str, job_id:str, data, path:str):
    """Write Job file

    Write text or json data to a file in a job directory directly.

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].send_job_data(job_id, data, path)
    except ValueError as v:
        # Raise 400 if bad data type to write
        raise falcon.HTTPError(falcon.HTTP_400, "jobs", str(v))
    except FileNotFoundError as f:
        # Raise 404 if path does not exist in job directory
        raise falcon.HTTPError(falcon.HTTP_404, "jobs", str(f))
    except Exception as e:
        # Raise 500 if unable to read file
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.get('/{jm_id}/jobs/{job_id}/files/peak')
def peak_job_file(jm_id:str, job_id:str, path:str, head:int=-1, tail:int=-1):
    """Peak Job File

    Extract first or last lines of a file in job directory via head/tail command.
    """
    _check_init(jm_id)

    try:
        return JM[jm_id].peak_job_file(job_id, path, head=head, tail=tail)
    except FileNotFoundError as f:
        # Raise 404 not found error if job file don't exist
        raise falcon.HTTPError(falcon.HTTP_404, "jobs", str(f))
    except Exception as e:
        # Raise 500 if some other error with peaking at file
        raise falcon.HTTPError(falcon.HTTP_500, "jobs", str(e))


@hug.get('/{jm_id}/scripts/list')
def list_scripts(jm_id:str):
    """List Scripts

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].list_scripts()
    except Exception as e:
        # Raise 500 if any error deploying script
        raise falcon.HTTPError(falcon.HTTP_500, "scripts", str(e))


@hug.post('/{jm_id}/scripts/deploy')
def deploy_script(jm_id:str, script_name:str, local_file:str=None):
    """Deploy Script

    """
    _check_init(jm_id)

    try:
        JM[jm_id].deploy_script(script_name, local_file=local_file)
    except Exception as e:
        # Raise 500 if any error deploying script
        raise falcon.HTTPError(falcon.HTTP_500, "scripts", str(e))


@hug.put('/{jm_id}/scripts/run')
def run_script(jm_id:str, script_name:str, job_id:str=None, args:[str]=None):
    """Run Script

    """
    _check_init(jm_id)

    try:
        return JM[jm_id].run_script(script_name, job_id=job_id, args=args)
    except Exception as e:
        # Raise 500 if any error running script
        raise falcon.HTTPError(falcon.HTTP_500, "scripts", str(e))


