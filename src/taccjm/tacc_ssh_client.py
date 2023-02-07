"""
TACCJM Client

Client for managing TACCJM hug servers and accessing TACCJM API end points.
"""
import os
import sys
import pdb
import re
import uvicorn
from prettytable import PrettyTable
import json
import psutil
import logging
import requests
import tempfile
import subprocess
from time import sleep
from getpass import getpass
from taccjm.constants import (
    make_taccjm_dir,
    TACC_SSH_HOST,
    TACC_SSH_PORT,
    TACCJM_SOURCE,
    TACCJM_DIR,
)
from typing import List, Tuple
from taccjm.exceptions import TACCJMError
from taccjm.utils import validate_file_attrs, filter_files

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Make log dirs and initialize logger
logger = logging.getLogger(__name__)


def _print_res(res, fields, search=None, match=r"."):
    """
    Print results

    Prints dictionary keys in list `fields` for each dictionary in res,
    filtering on the search column if specified with regular expression
    if desired.

    Parameters
    ----------
    res : List[dict]
        List of dictionaries containing response of an AgavePy call
    fields : List[string]
        List of strings containing names of fields to extract for each element.
    search : string, optional
        String containing column to perform string patter matching on to
        filter results.
    match : str, default='.'
        Regular expression to match strings in search column.

    """
    # Initialize Table
    x = PrettyTable()
    x.field_names = fields

    # Build table from results
    for r in res:
        if search is not None:
            if re.search(match, r[search]) is not None:
                x.add_row([r[f] for f in fields])
        else:
            x.add_row([r[f] for f in fields])

    # Print Table
    print(x)


def set_host(host: str = TACC_SSH_HOST, port: int = TACC_SSH_PORT) -> Tuple[str, int]:
    """
    Set Host

    Set where to look for a TACCJM server to be running. Note that this does not
    start/stop any servers on old or new host/port combinations.

    Parameters
    ----------
    host : str, default=`TACC_SSH_PORT`
        Host where taccjm server is running.
    PORT : int, default=`TACC_SSH_HOST`
        Port on host which taccjm server is listening for requests.

    Returns
    -------
    host_port : tuple of str or int
        Tuple containg (host, port) of new TACC_SSH_HOST and TACC_SSH_PORT.

    Warnings
    --------
    This method will not kill any existing taccjm servers running on previously
    set host/port.

    """
    global TACC_SSH_HOST, TACC_SSH_PORT
    TACC_SSH_HOST = host
    TACC_SSH_PORT = int(port)
    logger.info(f"Switched host {TACC_SSH_HOST} and port {TACC_SSH_PORT}")

    return (TACC_SSH_HOST, TACC_SSH_PORT)


def find_tjm_processes(start: bool = False, kill: bool = False) -> dict:
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
    srv_cmd = f"python {TACCJM_SOURCE}/tacc_ssh_server.py"
    srv_cmd += f" {TACC_SSH_HOST} {TACC_SSH_PORT}"

    # TODO: implement heartbeat?
    # hb_cmd = f"python {os.path.join(TACCJM_SOURCE, 'taccjm_server_heartbeat.py')}"
    # hb_cmd += f" --host={TACC_SSH_HOST} --port={TACC_SSH_PORT}"

    for proc in psutil.process_iter(["name", "pid", "cmdline"]):
        if proc.info["cmdline"] is not None:
            cmd = " ".join(proc.info["cmdline"])
            if srv_cmd in cmd:
                logger.info(f"Found server process at {proc.info['pid']}")
                processes_found["server"] = proc
            # TODO: implement heartbeat
            # if hb_cmd in cmd:
            #     logger.info(f"Found heartbeat process at {proc.info['pid']}")
            #     processes_found["hb"] = proc

    if kill:
        # Kill processes found and return empty dictionary
        for key, val in processes_found.items():
            msg = f"Killing {key} process with pid {val.info['pid']}"
            logger.info(msg)
            val.terminate()
        processes_found = {}

    if start:
        srv_log = os.path.join(
            TACCJM_DIR, f"taccjm_server_{TACC_SSH_HOST}_{TACC_SSH_PORT}.log"
        )
        # hb_log = os.path.join(
        #     TACCJM_DIR, f"taccjm_heartbeat_{TACC_SSH_HOST}_{TACC_SSH_PORT}.log"
        # )
        # for p in [("server", srv_cmd, srv_log), ("hb", hb_cmd, hb_log)]:
        for p in [("server", srv_cmd, srv_log)]:
            if p[0] not in processes_found.keys():
                # Start server/hb if not found
                with open(p[2], "w") as log:
                    processes_found[p[0]] = subprocess.Popen(
                        p[1].split(" "), stdout=log, stderr=subprocess.STDOUT
                    )
                    pid = processes_found[p[0]].pid
                    logger.info(f"Started {p[0]} process with pid {pid}")

    # Return processes found/started
    return processes_found


def api_call(http_method: str, end_point: str, params: dict = None,
             data: dict = None, json_data: dict = None) -> dict:
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
    base_url = "http://{host}:{port}".format(host=TACC_SSH_HOST, port=TACC_SSH_PORT)
    req = requests.Request(http_method, base_url + "/" + end_point, json=json_data, params=params, data=data)
    prepared = req.prepare()

    # Initialize connection and send http request
    s = requests.Session()

    try:
        res = s.send(prepared)
    except requests.exceptions.ConnectionError as c:
        logger.info("Cannot connect to server. Restarting and waiting 5s.")
        _ = find_tjm_processes(start=True)
        sleep(5)
        res = s.send(prepared)


    # Return content if success, else raise error
    if res.status_code == 200:
        return json.loads(res.text)
    else:
        pdb.set_trace()
        raise TACCJMError(res)


def list_ssh() -> List[str]:
    """
    List SSH Connections

    List available SSH sessions managed by ssh server.

    Parameters
    ----------

    Returns
    -------
    connection_ids : list of str
        List of connection IDs for ssh sessions available.
    """
    try:
        res = api_call("GET", "list")
    except TACCJMError as e:
        e.message = "list_jm error"
        logger.error(e.message)
        raise e

    return res


def init(
    connection_id: str,
    system: str,
    user: str = None,
    psw: str = None,
    mfa: str = None,
    restart=False,
) -> dict:
    """
    Init JM

    Initialize a JM instance on TACCJM server. If no TACCJM server is found
    to connect to, then starts the server.

    Parameters
    ----------
    connection_id: str
        ID to give to TACCSSHClient instance on the ssh server. Must be unique
        and not exist already in when executing `list_ssh()`.
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
    ssh_cnofig : dict
        Dictionary containing info about job manager instance just initialized.
    """
    connections = list_ssh()
    if connection_id in [c["id"] for s in connections]:
        raise ValueError(f"SSH Session {connection_id} already exists.")

    # Get user credentials/psw/mfa if not provided
    user = input("Username: ") if user is None else user
    psw = getpass("psw: ") if psw is None else psw
    mfa = input("mfa: ") if mfa is None else mfa
    data = {
        "system": system,
        "user": user,
        "psw": psw,
        "mfa": mfa,
        "restart": restart,
    }

    # Make API call
    try:
        res = api_call("POST", connection_id, json_data=data)
    except TACCJMError as e:
        e.message = "init_jm error"
        logger.error(e.message)
        raise e

    return res


def get(connection_id: str) -> dict:
    """
    Get SSH Connection

    Get info about a SSH session initialized on server.

    Parameters
    ----------
    connection_od : str
        ID of SSH connection to get.

    Returns
    -------
    connection_config : dictionary
        Dictionary containing SSH connection info.
    """

    try:
        res = api_call("GET", connection_id)
    except TACCJMError as e:
        e.message = f"get_jm error"
        logger.error(e.message)
        raise e

    return res


def list_files(
    connection_id: str,
    path: str = ".",
    attrs: List[str] = ["filename"],
    hidden: bool = False,
    search: str = None,
    match: str = r".",
) -> List[dict]:
    """
    List Files

    List files in a directory on remote system Job Manager is connected to.

    Parameters
    ----------
    connection_id : str
        ID of SSH Connection.
    path : str, default='.'
        Path to get files from. Defaults to user's home path on remote system.

    Returns
    -------
    files : dict
        List of files/folder in directory
    """
    attrs = validate_file_attrs(attrs)
    if search is not None and search not in attrs:
        raise ValueError(f"search must be one of attrs {attrs}")

    try:
        files = api_call("GET", f"{connection_id}/files/list", {"path": path})
    except TACCJMError as e:
        e.message = "list_files error"
        logger.error(e.message)
        raise e

    files = filter_files(files, attrs=attrs, hidden=hidden, search=search, match=match)

    return files

def exec(connection_id: str, cmnd: str,
         wait: bool = True):
    """
    Exec a command
    """

    json_data = {'cmnd': cmnd,
                 'wait': wait}


    # Make API call
    try:
        res = api_call("POST", f"{connection_id}/exec", json_data=json_data)
    except TACCJMError as e:
      e.message = f"Error executing command {cmnd}"
      logger.error(e.message)
      raise e


    return res


def process(connection_id: str, cmnd_id: int, nbytes: int = None,
         wait: bool = True):
    """
    Process a command
    """

    json_data = {'cmnd_id': cmnd_id,
                 'nbytes': nbytes,
                 'wait': wait}


    # Make API call
    try:
        res = api_call("POST", f"{connection_id}/process", json_data=json_data)
    except TACCJMError as e:
        e.message = f"Error processing command {cmnd}"
        logger.error(e.message)
        raise e

    return res


def upload(
    connection_id: str, local_path: str, remote_path: str, file_filter: str = "*"
) -> None:
    """
    Upload

    Upload file/folder from local path to remote path on system job manager is
    connected to.

    Parameters
    ----------
    connection_id : str
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
    """
    data = {
        "local_path": local_path,
        "remote_path": remote_path,
        "file_filter": file_filter,
    }
    try:
        api_call("PUT", f"{connection_id}/files/upload", data)
    except TACCJMError as e:
        e.message = "upload error"
        logger.error(e.message)
        raise e


def download(
    connection_id: str, remote_path: str, local_path: str, file_filter: str = "*"
) -> str:
    """
    Download

    Download file/folder from remote path on system job manager is connected to
    to local path.

    Parameters
    ----------
    connection_id : str
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
    data = {
        "remote_path": remote_path,
        "local_path": local_path,
        "file_filter": file_filter,
    }
    try:
        res = api_call("GET", f"{connection_id}/files/download", data)
    except TACCJMError as e:
        e.message = "download error"
        logger.error(e.message)
        raise e

    return res


def remove(connection_id: str, remote_path: str):
    """
    Remove file/folder

    Remove path on remote system job manager is connected to by moving it into
    the trash directory. Can restore file just removed withe `restore` method.

    Parameters
    ----------
    connection_id : str
        ID of Job Manager instance.
    remote_path : str
        Path of on remote system to file/folder to send to trash directory.

    Returns
    -------
    """
    data = {"remote_path": remote_path}
    try:
        res = api_call("DELETE", f"{connection_id}/files/remove", data)
    except TACCJMError as e:
        e.message = "remove error"
        logger.error(e.message)
        raise e

    return res


def restore(connection_id: str, remote_path: str):
    """
    Restore file/folder

    Restore the file at `remote_path` that was removed previously by a `remove`
    command. This moves the file/folder out of trash and back to its original
    path that is passed in.

    Parameters
    ----------
    connection_id : str
        ID of Job Manager instance.
    remote_path : str
        Path on remote system to file/folder to restore from trash directory.

    Returns
    -------

    Warnings
    --------
    Will overwrite file/folder at remote_path if something exists there.
    """
    data = {"remote_path": remote_path}
    try:
        res = api_call("PUT", f"{connection_id}/files/restore", data)
    except TACCJMError as e:
        e.message = "restore error"
        logger.error(e.message)
        raise e

    return res


def write(connection_id: str, data, remote_path: str):
    """
    Write File

    Write text (str) or json (dictionary) data directly to a file on remote
    system Job Manager is connected to.

    Parameters
    ----------
    connection_id : str
        ID of Job Manager instance.
    data : str or dict
        Text or json data to write to file.
    remote_path : str
        Path on remote system to write.

    Returns
    -------
    """
    data = {"data": data, "remote_path": remote_path}
    try:
        res = api_call("PUT", f"{connection_id}/files/write", data)
    except TACCJMError as e:
        e.message = "write error"
        logger.error(e.message)
        raise e

    return res


def read(connection_id: str, remote_path: str, data_type: str = "text"):
    """
    Read File

    Read text (str) or json (dictionary) data directly from a file on remote
    system Job Manager is connected to.

    Parameters
    ----------
    connection_id : str
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
    data = {"remote_path": remote_path, "data_type": data_type}
    try:
        res = api_call("GET", f"{connection_id}/files/read", data)
    except TACCJMError as e:
        e.message = "read error"
        logger.error(e.message)
        raise e

    return res


