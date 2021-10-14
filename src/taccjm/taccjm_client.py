"""
TACCJM Client

Client for managing TACCJM hug servers and accessing TACCJM API end points.
"""
import os
import pdb
import json
import psutil
import logging
import requests
import subprocess
from time import sleep
from getpass import getpass
from taccjm.constants import *
from typing import List, Tuple

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Initialize logger
logger = logging.getLogger(__name__)

class TACCJMError(Exception):
    """
    Custom TACCJM exception for errors encountered when interacting with
    commands sent to TACCJM server via HTTP endpoints.

    Attributes
    ----------
    jm_id : str
        TACC Job Manager which command was sent to.
    user : str
        User that sent API call.
    res : requests.models.Response
        Response object containing info on API call that failed.
    message : str
        Str message explaining error.
    """

    def __init__(self, res, message:str="API Error"):
        self.res = res
        self.message = message
        super().__init__(self.message)


    def __str__(self):
        # Get response object
        res = self.res

        # Format errors
        m =  f"{self.message} - {res.status_code} {res.reason}:\n"
        m += '\n'.join([f"{k} : {v}" for k,v in res.json()['errors'].items()])

        # Format HTTP request
        m += "\n-----------START-----------\n"
        m += f"{res.request.method} {res.request.url}\r\n"
        m += '\r\n'.join('{}: {}'.format(k, v) for k,
                v in res.request.headers.items())
        if res.request.body is not None:
            # Fix body to remove psw if exists, don't want in logs
            body = [s.split('=') for s in res.request.body.split('&')]
            body = [x if x[0]!='psw' else (x[0], '') for x in body]
            body = '&'.join(['='.join(x) for x in body])
            m += body

        return m


def set_host(host:str=TACCJM_HOST, port:int=TACCJM_PORT) -> Tuple[str, int]:
    """
    Set Host

    Set where to look for a TACCJM server to be running. Note that this does not
    start/stop any servers on old or new host/port combinations.

    Parameters
    ----------
    host : str, default=`TACCJM_PORT`
        Host where taccjm server is running.
    PORT : int, default=`TACCJM_HOST`
        Port on host which taccjm server is listening for requests.

    Returns
    -------
    host_port : tuple of str or int
        Tuple containg (host, port) of new TACCJM_HOST and TACCJM_PORT.

    Warnings
    --------
    This method will not kill any existing taccjm servers running on previously
    set host/port.

    """
    global TACCJM_HOST, TACCJM_PORT
    TACCJM_HOST = host
    TACCJM_PORT = int(port)
    logger.info(f"Switched host {TACCJM_HOST} and port {TACCJM_PORT}")

    return (TACCJM_HOST, TACCJM_PORT)


def find_tjm_processes(start:bool=False, kill:bool=False) -> dict:
    """
    Find TACC Job Manager Processes

    Looks for local processes that correspond to taccjm server and hearbeat.

    Parameters
    ----------
    start : bool, default=False
        Whether to start either server or heartbeat processes if they are not
        found. Note that if both start and kill are True then each processes
        will effectively restart if it exists.
    kill : bool, default=False
        Whether to kill the processes if they are found.

    Returns
    -------
    processes : dict
        Dictionary with keys `server` and/or `heartbeat' containing Process
        objects of processes corresponding to TACC Job Manager processes
        found/started. Note if kill is True but start is False, then this
        dictionary will always empty.

    """
    processes_found = {}

    # Strings defining commands
    srv_cmd = f"hug -ho {TACCJM_HOST} -p {TACCJM_PORT} -f "
    srv_cmd += os.path.join(TACCJM_SOURCE, 'taccjm_server.py')
    hb_cmd = "python "+os.path.join(TACCJM_SOURCE, 'taccjm_server_heartbeat.py')
    hb_cmd += f" --host={TACCJM_HOST} --port={TACCJM_PORT}"

    for proc in psutil.process_iter(['name', 'pid', 'cmdline']):
        if proc.info['cmdline']!=None:
            cmd = ' '.join(proc.info['cmdline'])
            if srv_cmd in cmd:
                logger.info(f"Found server process at {proc.info['pid']}")
                processes_found['server'] = proc
            if hb_cmd in cmd:
                logger.info(f"Found heartbeat process at {proc.info['pid']}")
                processes_found['hb'] = proc

    if kill:
        # Kill processes found and return empty dictionary
        for key,val in processes_found.items():
            msg = f"Killing {key} process with pid {val.info['pid']}"
            logger.info(msg)
            val.terminate()
        processes_found = {}

    if start:
        srv_log = os.path.join(TACCJM_DIR,
                f"taccjm_server_{TACCJM_HOST}_{TACCJM_PORT}.log")
        hb_log = os.path.join(TACCJM_DIR,
                f"taccjm_heartbeat_{TACCJM_HOST}_{TACCJM_PORT}.log")
        for p in [('server', srv_cmd, srv_log), ('hb', hb_cmd, hb_log)]:
            if p[0] not in processes_found.keys():
                # Start server/hb if not found
                with open(p[2], 'w') as log:
                    processes_found[p[0]] = subprocess.Popen(
                            p[1].split(' '),
                            stdout=log,
                            stderr=subprocess.STDOUT)
                    pid = processes_found[p[0]].pid
                    logger.info(f"Started {p[0]} process with pid {pid}")

    # Return processes found/started
    return processes_found


def api_call(http_method:str, end_point:str, data:dict=None) -> dict:
    """
    API Call

    Wrapper for general http call to taccjm server

    Parameters
    ----------
    http_method : str
        HTTP method to use.
    end_point : str
        URL end_point of resource to access with http method.
    data : dict
        Data to send along with http request.

    Returns
    -------
    p : dict
        Json returned from http method call.

    Raises
    ------
    TACCJMError
        Json returned from http method call.
    """

    # Build http request
    base_url = 'http://{host}:{port}'.format(host=TACCJM_HOST, port=TACCJM_PORT)
    req = requests.Request(http_method, base_url + '/' + end_point , data=data)
    prepared = req.prepare()

    # Initialize connection and send http request
    s = requests.Session()

    try:
        res = s.send(prepared)
    except requests.exceptions.ConnectionError as c:
        logger.info('Cannot connect to server. Restarting and waiting 5s.')
        _ = find_tjm_processes(start=True)
        sleep(5)
        res = s.send(prepared)

    # Return content if success, else raise error
    if res.status_code == 200:
        return json.loads(res.text)
    else:
        raise TACCJMError(res)


def list_jms() -> List[str]:
    """
    List JMs

    List available job managers managed by job manager server.

    Parameters
    ----------

    Returns
    -------
    jms : list of str
        List of job managers avaiable.
    """
    try:
        res = api_call('GET', 'list')
    except TACCJMError as e:
        e.message = "list_jm error"
        logger.error(e.message)
        raise e

    return res


def init_jm(jm_id:str, system:str,
        user:str=None, psw:str=None, mfa:str=None) -> dict:
    """
    Init JM

    Initialize a JM instance on TACCJM server. If no TACCJM server is found
    to connect to, then starts the server.

    Parameters
    ----------
    jm_id : str
        ID to give to Job Manager instance on TACCJM server. Must be unique and
        not exist already in TACCJM when executing `list_jms()`.
    system : str
        Name of tacc system to connect to. Must be one of stampede2, ls5,
        frontera, or maverick2.
    user : str, optional
        TACC user name. If non given, an input prompt will be provided.
    psw : str, optional
        TACC psw for user. if non given, a secure prompt will be provided.
    mfa : str, optional
        2-factor authentication code required to connect to TACC system. If
        non is provided, a prompt will be provided. Note, since code is on
        timer, give the server ample time to connect to TACC by copying the
        code when the timer just starts

    Returns
    -------
    jm : dict
        Dictionary containing info about job manager instance just initialized.
    """
    if jm_id in list_jms():
        raise ValueError(f"{jm_id} already exists.")

    # Get user credentials/psw/mfa if not provided
    user = input("Username: ") if user is None else user
    psw = getpass("psw: ") if psw is None else psw
    mfa = input("mfa: ") if mfa is None else mfa
    data = {'jm_id':jm_id, 'system': system,
            'user': user, 'psw': psw, 'mfa':mfa}

    # Make API call
    try:
        res = api_call('POST', 'init', data)
    except TACCJMError as e:
        e.message = "init_jm error"
        logger.error(e.message)
        raise e

    return res


def get_jm(jm_id:str) -> dict:
    """
    Get JM

    Get info about a Job Manager initialized on server.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.

    Returns
    -------
    jm : dictionary
        Dictionary containing job manager info.
    """

    try:
        res = api_call('GET', jm_id)
    except TACCJMError as e:
        e.message = f"get_jm error"
        logger.error(e.message)
        raise e

    return res


def get_queue(jm_id:str, user:str=None) -> dict:
    """
    Get Queue

    Get job queue info for system job manager is connected to.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    user : str, optional
        User to get job queue info about. Will by default get for user
        who initialized connection to job manager. Pass `all` as the user to
        get the whole job queue.

    Returns
    -------
    queue : dictionary
        Dictionary containing job manager info.
    """

    data = {'user': user} if user is not None else {}
    try:
        queue = api_call('GET', f"{jm_id}/queue", data)
    except TACCJMError as e:
        e.message = f"get_queue error"
        logger.error(e.message)
        raise e

    return queue


def get_allocations(jm_id:str) -> dict:
    """
    Get Allocations

    Get project allocations for user currently connected to remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.

    Returns
    ------
    allocations : dictionary
        Dictionary containing information on available project allocations.
    """

    try:
        allocations = api_call('GET', f"{jm_id}/allocations")
    except TACCJMError as e:
        e.message = f"get_allocations error"
        logger.error(e.message)
        raise e

    return allocations


def list_files(jm_id:str, path:str='~') -> List[str]:
    """
    List Files

    List files in a directory on remote system Job Manager is connected to.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    path : str, default='~'
        Path to get files from. Defaults to user's home path on remote system.

    Returns
    -------
    files : list of str
        List of files/folder in directory
    """

    try:
        res = api_call('GET', f"{jm_id}/files/list", {'path': path})
    except TACCJMError as e:
        e.message = "list_files error"
        logger.error(e.message)
        raise e

    return res


def peak_file(jm_id:str, path:str, head:int=-1, tail:int=-1) -> str:
    """
    Peak File

    Head at first or last lines of a file on remote system via the head/tail
    unix command.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    path : str
        Path of file to look at.
    head : int, default=-1
        If greater than 0, then get first `head` lines from file. If head and
        tail are both specified, head takes precedence.
    tail : int, default=-1
        If greater than 0, then get first `tail` lines from file. If head and
        tail are both specified, head takes precedence.

    Returns
    -------
    txt : str
        Text from first/last lines of file.
    """
    data = {'path': path, 'head': head, 'tail': tail}
    try:
        res = api_call('GET', f"{jm_id}/files/peak", data)
    except TACCJMError as e:
        e.message = "peak_file error"
        logger.error(e.message)
        raise e

    return res


def upload(jm_id:str, local_path:str,
        remote_path:str, file_filter:str='*') -> str:
    """
    Upload

    Upload file/folder from local path to remote path on system job manager is
    connected to.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    local_path : str
        Path of local file/folder to upload.
    remote_path : str
        Path of on remote system to put file/folder.
    file_filter : str, default='*'
        If uploading a directory, only files/folders that match the filter will
        be uploaded.

    Returns
    -------
    path : str
        Path on remote system of file/folder just uploaded.
    """
    data = {'local_path': local_path,
            'remote_path': remote_path,
            'file_filter': file_filter}
    try:
        res = api_call('GET', f"{jm_id}/files/upload", data)
    except TACCJMError as e:
        e.message = "upload error"
        logger.error(e.message)
        raise e

    return res


def download(jm_id:str, remote_path:str,
        local_path:str, file_filter:str='*') -> str:
    """
    Download

    Download file/folder from remote path on system job manager is connected to
    to local path.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    remote_path : str
        Path of on remote system to file/folder to download.
    local_path : str
        Path on local system to place file/folder downloaded.
    file_filter : str, default='*'
        If downloading a directory, only files/folders that match the filter
        will be downloaded.

    Returns
    -------
    path : str
        Path on local system to file/folder just downloaded.
    """
    data = {'remote_path': remote_path,
            'local_path': local_path,
            'file_filter': file_filter}
    try:
        res = api_call('GET', f"{jm_id}/files/download", data)
    except TACCJMError as e:
        e.message = "download error"
        logger.error(e.message)
        raise e

    return res


def remove(jm_id:str, remote_path:str):
    """
    Remove file/folder

    Remove path on remote system job manager is connected to by moving it into
    the trash directory. Can restore file just removed withe `restore` method.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    remote_path : str
        Path of on remote system to file/folder to send to trash directory.

    Returns
    -------
    """
    data = {'remote_path': remote_path}
    try:
        res = api_call('DELETE', f"{jm_id}/files/remove", data)
    except TACCJMError as e:
        e.message = "remove error"
        logger.error(e.message)
        raise e

    return res


def restore(jm_id:str, remote_path:str):
    """
    Restore file/folder

    Restore the file at `remote_path` that was removed previously by a `remove`
    command. This moves the file/folder out of trash and back to its original
    path that is passed in.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    remote_path : str
        Path on remote system to file/folder to restore from trash directory.

    Returns
    -------

    Warnings
    --------
    Will overwrite file/folder at remote_path if something exists there.
    """
    data = {'remote_path': remote_path}
    try:
        res = api_call('GET', f"{jm_id}/files/restore", data)
    except TACCJMError as e:
        e.message = "restore error"
        logger.error(e.message)
        raise e

    return res


def write(jm_id:str, data, remote_path:str):
    """
    Write File

    Write text (str) or json (dictionary) data directly to a file on remote
    system Job Manager is connected to.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    data : str or dict
        Text or json data to write to file.
    remote_path : str
        Path on remote system to write.

    Returns
    -------
    """
    data = {'data': data,
            'remote_path': remote_path}
    try:
        res = api_call('GET', f"{jm_id}/files/write", data)
    except TACCJMError as e:
        e.message = "write error"
        logger.error(e.message)
        raise e

    return res


def read(jm_id:str, remote_path:str, data_type:str='text'):
    """
    Read File

    Read text (str) or json (dictionary) data directly from a file on remote
    system Job Manager is connected to.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    remote_path : str
        Path on remote system to write.
    data_type : str, default='text'
        What tpye of data is contained in file to be read. Either `text` or
        `json`.

    Returns
    -------
    contents : str or dict
        Contents of file read.
    """
    data = {'remote_path': remote_path, 'data_type': data_type}
    try:
        res = api_call('GET', f"{jm_id}/files/read", data)
    except TACCJMError as e:
        e.message = "read error"
        logger.error(e.message)
        raise e

    return res


def list_apps(jm_id:str):
    """
    List Apps

    List available applications deployed on remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.

    Returns
    -------
    apps : list of str
        List of applications deployed on remote system
    """
    try:
        res = api_call('GET', f"{jm_id}/apps/list")
    except TACCJMError as e:
        e.message = "list_apps error"
        logger.error(e.message)
        raise e

    return res


def get_app(jm_id:str, app_id:str):
    """
    Get Application

    Get application config for application deployed on remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    app_id : str
        ID of application deployed on remote system.

    Returns
    -------
    app_config : dictionary
        Dictionary containing application configuration info.
    """

    try:
        res = api_call('GET', f"{jm_id}/apps/{app_id}")
    except TACCJMError as e:
        e.message = f"get_app error"
        logger.error(e.message)
        raise e

    return res

def deploy_app(jm_id:str, app_config:dict,
        local_app_dir:str='.', overwrite:bool=False):
    """
    Deploy Application

    Deploy an application to remote system managed by job manager.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    app_config : dict
        Dictionary containing configurations for application.
    local_app_dir : str
        Local path containing application `assets` directory to send to remote
        system.
    overwrite : bool, default=False
        Whether to overwrite application on remote system if it already exists
        (same application name and version).

    Returns
    -------
    app_config : dictionary
        Dictionary containing application configuration info of application
        just deployed.
    """

    data = {'app_config': app_config,
            'local_app_dir': local_app_dir,
            'overwrite': overwrite}
    try:
        res = api_call('GET', f"{jm_id}/apps/deploy", data)
    except TACCJMError as e:
        e.message = f"deploy_app error"
        logger.error(e.message)
        raise e

    return res


def list_jobs(jm_id:str):
    """
    List Jobs

    List jobs deployed on remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.

    Returns
    -------
    jobs : list of str
        List of jobs deployed on remote system
    """
    try:
        res = api_call('GET', f"{jm_id}/jobs/list")
    except TACCJMError as e:
        e.message = "list_jobs error"
        logger.error(e.message)
        raise e

    return res


def get_job(jm_id:str, job_id:str):
    """
    Get Job

    Get job config for job deployed on remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_id : str
        ID of job deployed on remote system.

    Returns
    -------
    job_config : dictionary
        Dictionary containing job configuration info.
    """

    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}")
    except TACCJMError as e:
        e.message = f"get_job error"
        logger.error(e.message)
        raise e

    return res


def deploy_job(jm_id:str, job_config:dict):
    """
    Deploy Job

    Deploy a job to remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_config : dict
        Dictionary containing configurations for job.

    Returns
    -------
    job_config : dictionary
        Dictionary containing job configuration info of job just deployed.
    """

    try:
        res = api_call('GET', f"{jm_id}/jobs/deploy", {'job_config':job_config})
    except TACCJMError as e:
        e.message = f"deploy_job error"
        logger.error(e.message)
        raise e

    return res


def submit_job(jm_id:str, job_id:str):
    """
    Submit Job

    Submit a deployed job to HPC job queue on remote system.

    Parameters
    ----------
    jm_id : str
        ID of job manager instance where job is deployed.
    job_id : str
        ID of job deployed on remote system to submit.

    Returns
    -------
    job_config : dictionary
        Dictionary containing updated configuration of job just submitted.
    """

    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/submit")
    except TACCJMError as e:
        e.message = f"submit_job error"
        logger.error(e.message)
        raise e

    return res


def cancel_job(jm_id:str, job_id:str):
    """
    Cancel Job

    Canel a job that has been submitted to HPC job queue.

    Parameters
    ----------
    jm_id : str
        ID of job manager instance where job is deployed.
    job_id : str
        ID of job to cancel.

    Returns
    -------
    job_config : dictionary
        Dictionary containing updated configuration of job just cancelled.
    """

    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/cancel")
    except TACCJMError as e:
        e.message = f"cancel_job error"
        logger.error(e.message)
        raise e

    return res


def cleanup_job(jm_id:str, job_id:str):
    """
    Cleanup Job

    Cancel (if already submitted) and remove a job.

    Parameters
    ----------
    jm_id : str
        ID of job manager instance where job is deployed.
    job_id : str
        ID of job to cancel and remove.

    Returns
    -------
    job_id : str
        Job ID of job just removed.
    """

    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/cleanup")
    except TACCJMError as e:
        e.message = f"cleanup_job error"
        logger.error(e.message)
        raise e

    return res


def list_job_files(jm_id:str, job_id:str, path:str=''):
    """
    List Job Files

    List files in a job's directory.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_id : str
        ID of job.
    path : str, default=''
        Path to get files from relative to jobs root directory.

    Returns
    -------
    files : list of str
        List of files/folder in job directory
    """

    try:
        res = api_call('GET',
                f"{jm_id}/jobs/{job_id}/files/list", {'path': path})
    except TACCJMError as e:
        e.message = "list_job_files error"
        logger.error(e.message)
        raise e

    return res


def download_job_file(jm_id:str, job_id:str, path:str, dest_dir:str='.'):
    """
    Download Job File/Folder

    Download a file or folder from a job's directory.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_id : str
        ID of job.
    path : str
        Path, relative to job directory, to file/folder to download
    dest_dir : str, default='.'
        Local path to download job file/folder to. Defaults to current dir.
    file_filter : str, default='*'
        If downloading a directory, only files/folders that match the filter
        will be downloaded.

    Returns
    -------
    local_path : str
        Path on local system to file/folder just downloaded
    """

    data = {'path': path, 'dest_dir': dest_dir, 'file_filter': file_filter}
    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/files/download", data)
    except TACCJMError as e:
        e.message = "download_job_file error"
        logger.error(e.message)
        raise e

    return res


def upload_job_file(jm_id:str, job_id:str,
        path:str, dest_dir:str='.', file_filter='*'):
    """
    Upload Job File/Folder

    Upload a file or folder from a job's directory.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_id : str
        ID of job.
    path : str
        Local path to file/folder to upload.
    dest_dir : str, default='.'
        Path, relative to job directory, to upload file/folder to. Defaults to
        job's root directory.
    file_filter : str, default='*'
        If uploading a directory, only files/folders that match the filter will
        be uploaded.

    Returns
    -------
    local_path : str
        Path on local system to file/folder just downloaded
    """

    data = {'path': path, 'dest_dir': dest_dir, 'file_filter': file_filter}
    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/files/upload", data)
    except TACCJMError as e:
        e.message = "upload_job_file error"
        logger.error(e.message)
        raise e

    return res


def read_job_file(jm_id:str, job_id:str, path:str, data_type:str='text'):
    """
    Read Job File

    Read text (str) or json (dictionary) data directly from a file at `path`
    relative to `job_id`'s directory on remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_id : str
        ID of Job.
    path : str
        Path, relative to job directory, to read data from.
    data_type : str, default='text'
        What tpye of data is contained in file to be read. Either `text` or
        `json`.

    Returns
    -------
    contents : str or dict
        Contents of job file read.
    """
    data = {'path': path, 'data_type': data_type}
    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/files/read", data)
    except TACCJMError as e:
        e.message = "read_job_file error"
        logger.error(e.message)
        raise e

    return res


def write_job_file(jm_id:str, job_id:str, data, path:str):
    """
    Write Job File

    Write text (str) or json (dictionary) data directly to a file at `path`
    relative to `job_id`'s directory on remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_id : str
        ID of Job.
    data : str or dict
        Text or json data to write to file.
    path : str
        Path, relative to job directory, to write data to.

    Returns
    -------
    path : str or dict
        Path in job directory where file was written to.
    """
    data = {'data': data, 'path': path}
    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/files/write", data)
    except TACCJMError as e:
        e.message = "write_job_file error"
        logger.error(e.message)
        raise e

    return res


def peak_job_file(jm_id:str, path:str, head:int=-1, tail:int=-1):
    """
    Peak Job File

    Read at first or last lines of a file in a job's directory on remote system
    via the head/tail unix command.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    job_id : str
        ID of Job.
    path : str
        Path of file, relative to job directory, to look at.
    head : int, default=-1
        If greater than 0, then get first `head` lines from file. If head and
        tail are both specified, head takes precedence.
    tail : int, default=-1
        If greater than 0, then get first `tail` lines from file. If head and
        tail are both specified, head takes precedence.

    Returns
    -------
    txt : str
        Text from first/last lines of job file.
    """
    data = {'path': path, 'head': head, 'tail': tail}
    try:
        res = api_call('GET', f"{jm_id}/jobs/{job_id}/files/peak", data)
    except TACCJMError as e:
        e.message = "peak_job_file error"
        logger.error(e.message)
        raise e

    return res


def list_scripts(jm_id:str):
    """
    List Scripts

    List scripts deployed on remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.

    Returns
    -------
    scripts : list of str
        List of scripts deployed on remote system
    """
    try:
        res = api_call('GET', f"{jm_id}/scripts/list")
    except TACCJMError as e:
        e.message = "list_scripts error"
        logger.error(e.message)
        raise e

    return res


def deploy_script(jm_id:str, script_name:str, local_file:str=None):
    """
    Deploy Script

    Deploy a script to remote system.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    script_name : str
        The name of the script. Will be used as the local filename unless
        local_file is passed. If the filename ends in .py, it will be
        assumed to be a Python3 script. Otherwise, it will be treated as a
        generic executable.
    local_file : str
        The local filename of the script if not passed, will be inferred
        from script_name.

    Returns
    -------
    """

    data = {'script_name': script_name, 'local_file': local_file}
    try:
        res = api_call('GET', f"{jm_id}/scripts/deploy", data)
    except TACCJMError as e:
        e.message = f"deploy_script error"
        logger.error(e.message)
        raise e

    return res


def run_script(jm_id:str, script_name:str, job_id:str=None, args:[str]=None):
    """
    Run Script

    Run a pre-deployed script on TACC.

    Parameters
    ----------
    jm_id : str
        ID of Job Manager instance.
    script_name : str
        The name of the script, without file extensions.
    job_id : str
        Job Id of job to run the script on.  If passed, the job
        directory will be passed as the first argument to script.
    args : list of str
        Extra commandline arguments to pass to the script.

    Returns
    -------
    out : str
        The standard output of the script.
    """

    data = {'script_name': script_name, 'job_id': job_id,  'args': args}
    try:
        res = api_call('GET', f"{jm_id}/scripts/run", data)
    except TACCJMError as e:
        e.message = f"run_script error"
        logger.error(e.message)
        raise e

    return res
