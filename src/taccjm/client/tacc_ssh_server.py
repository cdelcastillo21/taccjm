"""
TACC SSH FastAPI Server

Server for managing instances of TACCSSHClient classes using the FastAPI framework
"""
import logging
from loguru import logger
import os
import pdb
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from taccjm.constants import TACCJM_DIR, make_taccjm_dir
from taccjm.client.TACCSSHClient import TACCSSHClient
from taccjm.utils import get_log_level, get_log_level_str, init_logger

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Initialize server logger
if not os.path.exists(TACCJM_DIR):
    os.makedirs(TACCJM_DIR)

app = FastAPI()

# Dictionary containing all job manager instances being managed
# Note there could be multiple instance if managing more than one system
HOST = None
PORT = None
CONNECTIONS = {}
LOGFILE = None
LOGLEVEL = None


def _get_config(connection_id):
    """
    Get the config associated with a connection ID if it exists.
    """
    if connection_id in CONNECTIONS.keys():
        res = CONNECTIONS[connection_id]
        res = {i: res[i] for i in res if i != "client"}
        return res
    else:
        raise ValueError(f"No active SSH Connection with id {connection_id}")


def _get_client(connection_id):
    if connection_id in CONNECTIONS.keys():
        return CONNECTIONS[connection_id]["client"]
    else:
        raise ValueError(f"No active SSH Connection with id {connection_id}")


class Connection(BaseModel):
    id: str
    sys: str
    user: str
    start: datetime
    last_ts: datetime
    log_level: str
    log_file: str
    home_dir: str
    work_dir: str
    scratch_dir: str


class ConnectionRequest(BaseModel):
    system: str
    user: str
    psw: str
    mfa: str
    restart: Union[bool, None] = False
    loglevel: Union[str, None] = None
    logfile: Union[str, None] = None


@app.get("/", response_model=List[Connection])
def list_jm():
    """Show initialized job managers"""
    out = []
    for c in CONNECTIONS.keys():
        conn = CONNECTIONS[c]
        out.append({a: conn[a] for a in conn if a != "client"})
    return out


@app.post("/{connection_id}", response_model=Connection)
async def init(connection_id: str, req: ConnectionRequest):
    if connection_id in CONNECTIONS.keys() and not req.restart:
        msg = f"Connection {connection_id} already exists."
        raise HTTPException(status_code=409, detail=msg)

    logger.info(f"Init {connection_id} with on {req.system} for {req.user}")
    loglevel = get_log_level(req.loglevel) if req.loglevel is not None else LOGLEVEL
    def_log = f"{TACCJM_DIR}/{connection_id}_{req.system}_{req.user}_log.json"
    logfile = req.logfile if req.logfile is not None else def_log
    logger.info(f"Log at {logfile} with level {get_log_level_str(loglevel)}")

    try:
        client = TACCSSHClient(
            req.system,
            user=req.user,
            psw=req.psw,
            mfa=req.mfa,
            log_config={"output": logfile, "fmt": "json", "level": loglevel},
        )
    except ValueError as v:
        msg = f"Init failed on {req.system} for {req.user}: {v}"
        logger.error(msg)
        raise HTTPException(status_code=404, detail=f"ssh_error: {msg}")
    except Exception as e:
        msg = f"Init failed on {req.system} for {req.user}: {e}"
        logger.error(msg)
        raise HTTPException(status_code=401, detail=f"ssh_error: {msg}")

    ret = {
        "id": connection_id,
        "sys": client.system,
        "user": client.user,
        "start": datetime.now(),
        "last_ts": datetime.now(),
        "log_level": loglevel,
        "log_file": logfile,
        "scratch_dir": client.scratch_dir,
        "home_dir": client.home_dir,
        "work_dir": client.work_dir,
    }
    logger.info(
        f"SUCCESS - {connection_id} initialized establisehed.",
        extra={"connection_config": ret},
    )
    CONNECTIONS[connection_id] = ret
    CONNECTIONS[connection_id]["client"] = client

    return ret


@app.get("/{connection_id}", response_model=Connection)
def get(
    connection_id: str,
):
    if connection_id not in CONNECTIONS.keys():
        msg = f"Connection id {connection_id} not found"
        raise HTTPException(status_code=404, detail=f"ssh_error: {msg}")
    else:
        ret = CONNECTIONS[connection_id]
        ret = {a: ret[a] for a in ret if a != "client"}

        return ret


@app.delete("/{connection_id}")
def stop(
    connection_id: str,
):
    if connection_id not in CONNECTIONS.keys():
        msg = f"Connection id {connection_id} not found"
        raise HTTPException(status_code=404, detail=f"ssh_error: {msg}")
    else:
        connection = CONNECTIONS.pop(connection_id)
        connection['client'].close()


class CommandRequest(BaseModel):
    cmnd: str
    wait: Union[bool, None] = True
    key: Union[str, None] = None


class ProcessRequest(BaseModel):
    cmnd_id: Union[int, None] = None
    nbytes: Union[int, None] = None
    wait: Union[bool, None] = True


class Command(BaseModel):
    id: int
    key: str
    cmd: str
    ts: datetime
    status: str
    stdout: str
    stderr: str
    history: List[Dict]
    rt: Union[float, int, None]
    rc: Union[int, None]


@app.post("/{connection_id}/exec", response_model=Command)
def exec(connection_id: str, cmnd_req: CommandRequest):
    """Execute command"""
    ssh_client = _get_client(connection_id)
    logger.info(
        f"Executing new command on {connection_id}", extra={"command_request": cmnd_req}
    )
    res = ssh_client.execute_command(cmnd_req.cmnd,
                                     wait=cmnd_req.wait,
                                     error=False,
                                     key=cmnd_req.key)
    res = {i: res[i] for i in res if i != "channel"}
    CONNECTIONS[connection_id]["last_ts"] = datetime.now()
    logger.info(
        f"Command {res['id']} executed on {connection_id}.",
        extra={"command_config": res},
    )

    return res


@app.post("/{connection_id}/process", response_model=Union[Command, List[Command]])
def process(connection_id: str, proc_req: ProcessRequest):
    """Process command"""

    ssh_client = _get_client(connection_id)
    if proc_req.cmnd_id is not None:
        logger.info(
            f"Processing command {proc_req.cmnd_id} on {connection_id}",
            extra={"process_request": proc_req},
        )
        res = ssh_client.process_command(
            proc_req.cmnd_id, nbytes=proc_req.nbytes, wait=proc_req.wait, error=False
        )
        res = {i: res[i] for i in res if i != "channel"}
        logger.info(
            f"Command {res['id']} execute/processed on {connection_id}.",
            extra={"command_config": res},
        )
    else:
        logger.info("Polling all active commands", extra={"process_request": proc_req})
        res = ssh_client.process_active(nbytes=proc_req.nbytes)
        res = [{i: r[i] for i in r if i != "channel"} for r in res]
        logger.info(
            f"{len(res)} commands still active n {connection_id}.",
            extra={"active_commands": res},
        )

    CONNECTIONS[connection_id]["last_ts"] = datetime.now()

    return res


@app.get("/{connection_id}/commands",
         response_model=Union[Command, List[Command]])
def list_commands(connection_id: str):
    """Process command"""
    ssh_client = _get_client(connection_id)
    ssh_client.commands
    res = [{i: r[i] for i in r if i != "channel"} for r in ssh_client.commands]
    return res


class PathInfo(BaseModel):
    path: Union[str, None] = "."
    filename: Union[str, None] = None
    st_atime: Union[int, None] = None
    st_gid: Union[int, None] = None
    st_mode: Union[int, None] = None
    st_mtime: Union[int, None] = None
    st_size: Union[int, None] = None
    st_uid: Union[int, None] = None
    ls_str: Union[str, None] = None


@app.get("/{connection_id}/ls/{file_path:path}", response_model=List[PathInfo])
async def list_files(connection_id: str, file_path: str):
    """
    List Files

    List files at the given file path

    """
    client = _get_client(connection_id)

    try:
        res = client.list_files(file_path)
    except Exception as e:
        msg = f"Error accessing {file_path} not found : {e}"
        logger.error(msg)
        raise HTTPException(status_code=404, detail=msg)

    return res


@app.get("/{connection_id}/lsr/{file_path:path}", response_model=List[PathInfo])
async def list_files_recursive(connection_id: str, file_path: str):
    """
    List Files

    List files at the given file path

    """
    client = _get_client(connection_id)

    try:
        res = client.list_files(file_path, recurse=True)
    except Exception as e:
        msg = f"Error accessing {file_path} not found : {e}"
        logger.error(msg)
        raise HTTPException(status_code=404, detail=msg)

    return res


class FileData(BaseModel):
    path: str
    data_type: str = "text"
    data: Union[str, dict, None] = None


@app.get("/{connection_id}/read/{file_path:path}", response_model=FileData)
async def read(connection_id: str, file_path: str):
    """
    Read file
    """
    client = _get_client(connection_id)

    file = {"path": file_path}
    file["data_type"] = "json" if file["path"].endswith(".json") else "text"
    file["data"] = client.read(file["path"], data_type=file["data_type"])

    return file


@app.post("/{connection_id}/write")
async def write(connection_id: str, data: FileData):
    """
    Write file
    """
    client = _get_client(connection_id)
    client.write(data.data, data.path)


class DataRequest(BaseModel):
    source_path: str
    dest_path: str
    file_filter: str = "*"


@app.get("/{connection_id}/download")
async def download(
    connection_id: str, source_path: str, dest_path: str, file_filter: str = "*"
):
    """
    List Files

    List files at the given file path

    """
    client = _get_client(connection_id)
    client.download(source_path, dest_path, file_filter=file_filter)

    return dest_path


@app.post("/{connection_id}/upload")
async def upload(connection_id: str, req: DataRequest):
    """
    List Files

    List files at the given file path

    """
    client = _get_client(connection_id)
    try:
        client.upload(req.source_path, req.dest_path, file_filter=req.file_filter)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found {req.source_path}")
    except PermissionError:
        raise HTTPException(
            status_code=403, detail=f"Don't have permissions to {req.source_path}"
        )


if __name__ == "__main__":
    import uvicorn

    HOST = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
    LOGLEVEL = get_log_level(sys.argv[3]) if len(sys.argv) > 3 else logging.INFO

    LOGFILE = f"{TACCJM_DIR}/ssh_server_{HOST}_{PORT}_log.json"
    serialize = True if "fmt" == "json" else False
    logger.add(LOGFILE, rotation="10 MB", filter="tacc_ssh_server",
               serialize=serialize)
    logger.info("Starting TACC SSH Server.")
    uvicorn.run(app, host=HOST, port=PORT)
    logger.info("TACC SSH Server shut down.")
