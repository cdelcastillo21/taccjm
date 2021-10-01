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

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

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

    def __init__(self, res, message="API Error"):
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
        m += "-----------START-----------\n"
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


def set_host(host:str=TACCJM_HOST, port:int=TACCJM_PORT):
    """
    Set Host

    Set where to look for a TACCJM server to be running.

    Parameters
    ----------
    host : str, default=`TACCJM_PORT`
        Host where taccjm server is running.
    PORT : int, default=`TACCJM_HOST`
        Port on host which taccjm server is listening for requests.

    Returns
    -------

    Warnings
    --------
    This method will not kill any existing taccjm servers running on previously
    set host/port.

    """
    global TACCJM_HOST, TACCJM_PORT
    TACCJM_HOST = host
    TACCJM_PORT = int(port)
    logger.info(f"Switched host {TACCJM_HOST} and port {TACCJM_PORT}")


def find_tjm_processes(start=False, kill=False):
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


def api_call(http_method, end_point, data=None):
    """
    API Call

    Wrapper for general http call to taccjm server.

    Parameters
    ----------

    Returns
    -------
    p : dict
        Dictionary containing server and heartbeat processes found or started.
    """

    # Build http request
    base_url = 'http://{host}:{port}'.format(host=TACCJM_HOST, port=TACCJM_PORT)
    req = requests.Request(http_method, base_url + '/' + end_point , data=data)
    prepared = req.prepare()

    # Initialize connection and send http request
    s = requests.Session()

    try:
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
    except TACCJMError as t:
        raise t
    except Exception as e:
        logger.info(f"Unknown error when trying to connect to server - {e}")


def list_jms():
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
        user:str=None, psw:str=None, mfa:str=None):
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
    if jm_id in [j['jm_id'] for j in list_jms()]:
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


def get_jm(jm_id:str):
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


def get_queue(jm_id:str, user:str=None):
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

    try:
        queue = api_call('GET', f"{jm_id}/queue")
    except TACCJMError as e:
        e.message = f"get_jm error"
        logger.error(e.message)
        raise e

    return queue
#
#
# def load_app(app):
#   data = {'app': app}
# 
#   res = api_call('GET', 'apps/load', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - load_app error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def get_jobs(head=-1):
#   data = {'head': head}
# 
#   res = api_call('GET', 'jobs', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - get_jobs error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def load_job(job_id):
#   data = {'job_id': job_id}
# 
#   res = api_call('GET', 'jobs/load', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - load_job error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def save_job(job_config):
#   data = {'job_config': job_config}
# 
#   res = api_call('GET', 'jobs/save', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - save_job error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def setup_job(job_config):
#   # TODO: Do some error checking on job config?
#   data = {'job_config': job_config}
# 
#   res = api_call('POST', 'jobs/setup', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - setup_job error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def submit_job(job_id):
#   data = {'job_id': job_id}
# 
#   res = api_call('PUT', 'jobs/submit', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - submit_job error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def cancel_job(job_id):
#   data = {'job_id': job_id}
# 
#   res = api_call('PUT', 'jobs/cancel', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - cancel_job error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def cleanup_job(job_id):
#   data = {'job_id': job_id}
# 
#   res = api_call('DELETE', 'jobs/cleanup', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - cleanup_job error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def ls_job(job_id, path:str=None):
#   data = {'job_id': job_id,
#           'path': path}
# 
#   res = api_call('GET', 'jobs/ls', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - ls_job error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def get_job_file(job_id:str, fpath:str, dest_dir:str, head:int=-1, tail:int=-1):
#   data = {'job_id': job_id,
#           'fpath': fpath,
#           'dest_dir': dest_dir,
#           'head': head,
#           'tail': tail}
#   res = api_call('GET', 'jobs/file', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - get_job_file error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def put_job_file(job_id:str, fpath:str, dest_dir:str=None):
#   data = {'job_id': job_id,
#           'fpath': fpath}
#   if dest_dir!=None:
#       data['dest_dir']=dest_dir
# 
#   res = api_call('PUT', 'jobs/file', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - put_job_file error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 
# 
# def deploy_app(app_name:str, local_app_dir:str, version=None, overwrite=False):
#   data = {'app_name': app_name,
#           'local_app_dir': local_app_dir}
#   if version!=None:
#       data['version']=version
#   if overwrite!=None:
#       data['overwrite']=overwrite
# 
#   res = api_call('PUT', 'apps/deploy', data)
# 
#   if not res['status']:
#     msg = f"TACCJM - put_job_file error - {res}"
#     logger.error(msg)
#     raise Exception(msg)
# 
#   return res['res']
# 

# Start server upon loading library
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
