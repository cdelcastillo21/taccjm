"""
TACCJM Client

Client for managing TACCJM hug servers and accessing TACCJM API end points.
"""
import os
import pdb
import json
import psutil
import getpass
import logging
import requests
import subprocess
from taccjm.constants import *

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


def set_host(host=TACCJM_HOST, port=TACCJM_PORT):
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
    TACCJM_PORT = port
    logger.info(f"Switched host {TACCJM_HOST} and port {TACCJM_PORT}")


def find_tjm_processes():
    """
    Find TACC Job Manager Processes

    Looks for local processes that correspond to taccjm server and hearbeat.

    Parameters
    ----------

    Returns
    -------
    processes : dict
        Dictionary with keys `server` and/or `heartbeat' containing psutil
        process objects corresponding to TACC Job Manager processes found.

    """
    processes_found = {}

    # Strings defining commands
    server_cmd = f"hug -ho {TACCJM_HOST} -p {TACCJM_PORT} -f "
    server_cmd += os.path.join(TACCJM_SOURCE, 'taccjm_server.py')
    hb_cmd = "python "+os.path.join(TACCJM_SOURCE, 'taccjm_server_heartbeat.py')
    hb_cmd += f" --host={TACCJM_HOST} --port={TACCJM_PORT}"

    for proc in psutil.process_iter(['name', 'pid', 'cmdline']):
        if proc.info['cmdline']!=None:
            cmd = ' '.join(proc.info['cmdline'])
            if server_cmd in cmd:
                processes_found['server'] = proc
            if hb_cmd in cmd:
                processes_found['hb'] = proc

    return processes_found


def kill_server():
    """
    Kill Server

    Looks for and kills local processes that correspond to taccjm
    server and hearbeat.

    Parameters
    ----------

    Returns
    -------

    """
    # Search for server process
    p = find_tjm_processes()
    if 'server' in p.keys():
        # kill server
        msg = f"Killing server process with pid {p['server'].info['pid']}"
        logger.info(msg)
        p['server'].terminate()
    else:
        logger.info('Did not find server process to kill')

    if 'hb' in p.keys():
        # kill server
        msg = f"Killing heartbeat process with pid {p['hb'].info['pid']}"
        logger.info(msg)
        p['hb'].terminate()
    else:
        logger.info('Did not find heartbeat process to kill')


def check_start_server():
    """
    Check and Start Server

    Looks for taccjm server and heartbeat processes and starts them if
    they are not running.

    Parameters
    ----------

    Returns
    -------
    p : dict
        Dictionary containing server and heartbeat processes found or started.
    """
    # Commands to start hug server and heartbeat process
    server_cmd = f"hug -ho {TACCJM_HOST} -p {TACCJM_PORT} -f "
    server_cmd += os.path.join(os.path.dirname(taccjm.__file__),
            'taccjm_server.py')
    hb_cmd = "python "+os.path.join(TACCJM_SOURCE, 'taccjm_server_heartbeat.py')
    hb_cmd += f" --host={TACCJM_HOST} --port={TACCJM_PORT}"

    server_log = os.path.join(TACCJM_DIR,
            f"taccjm_server_{TACCJM_HOST}_{TACCJM_PORT}.log")
    heartbeat_log = os.path.join(TACCJM_DIR,
            f"taccjm_heartbeat_{TACCJM_HOST}_{TACCJM_PORT}.log")

    # Search for server process
    p = find_tjm_processes()
    if 'server' not in p.keys():
        # Start server
        with open(server_log, 'w') as log:
            p['server'] = subprocess.Popen(server_cmd.split(' '), stdout=log,
                    stderr=subprocess.STDOUT)
            logger.info(f"Started server process with pid {p['server'].pid}")
    else:
        logger.info('Found server process at ' + str(p['server'].info['pid']))

    # Search for heartbeat process
    if 'hb' not in p.keys():
        # Start heartbeat
        with open(heartbeat_log, 'w') as log:
            p['hb'] = subprocess.Popen(hb_cmd.split(' '),
                    stdout=log, stderr=subprocess.STDOUT)
            logger.info(f"Started heartbeat process with pid {p['hb'].pid}")
    else:
        logger.info(f"Found heartbeat process at {p['hb'].info['pid']}")

    return p


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
    base_url = 'http://{host}:{port}'.format(host=TACCJM_HOST, port=TACCJM_PORT)

    req = requests.Request(http_method, base_url + '/' + end_point , data=data)
    prepared = req.prepare()
    logger.info('{}\n{}\r\n{}\r\n\r\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\r\n'.join('{}: {}'.format(k, v) for k,
            v in req.headers.items()), req.body,))

    s = requests.Session()
    res = s.send(prepared)
    status = res.status_code

    if status==200:
        return json.loads(res.text)
    else:
        pdb.set_trace()
        # TODO: Clean this up, create appropriate exception
        logger.info('API Call Failed')
        raise Exception('API Call Failed')


def init_jm(jm_id:str, system:str,
        user:str=None, psw:str=None, mfa:str=None, restart=False):
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
    restart : bool, default=False
        If set to True, then any server found on the given host/prot combination
        will be killed and restarted first before initailizign new JM.

    Returns
    -------
    p : dict
        Dictionary containing server and heartbeat processes found or started.
    """

    # Kill any active server at currently set host/port if restart set
    if restart==True:
        kill_server()

    # Start server and heartbeat process if necessary
    check_start_server()

    # Try first to get allocations. If connected already this should work
    try:
        allocations = get_allocations()
    except:
        # Get user credentials/psw/mfa if not provided
        data = {'user': user,
                        'psw': psw,
                        'mfa':mfa}
        if data['user'] is None:
            data['user'] = input("Username: ")
        if data['psw'] is None:
            data['psw'] = getpass.getpass("Password: ")
        if data['mfa'] is None:
            data['mfa'] = input("TACC Token Code: ")

        # Make API call
        res = api_call('POST', 'init', data)

        # Check if successfully logged in
        if not res['status']:
            msg = f"TACCJM - init error - {res}"
            logger.error(msg)
            raise Exception(msg)

    return res['res']


def list_jm():
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
    except Exception as e:
        msg = f"list_jm - {e}"
        logger.error(msg)
        raise e

    return res

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
