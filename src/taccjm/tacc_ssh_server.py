"""
TACC SSH FastAPI Server

Server for managing instances of TACCSSHClient classes using the FastAPI framework
"""
import os
import pdb
import sys
import logging
from fastapi import FastAPI, HTTPException
from pythonjsonlogger import jsonlogger
from taccjm.TACCSSHClient import TACCSSHClient
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List, Dict, Union
from taccjm.utils import init_logger
from taccjm.constants import make_taccjm_dir, TACCJM_DIR


__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Initialize server logger
if not os.path.exists(TACCJM_DIR):
    os.makedirs(TACCJM_DIR)
logger = init_logger(__name__, output=f"{TACCJM_DIR}/ssh_server_log.json", fmt='json')

app = FastAPI()

# Dictionary containing all job manager instances being managed
# Note there could be multiple instance if managing more than one system
CONNECTIONS = {}

def _get_client(connection_id):
    if connection_id in CONNECTIONS.keys():
        return CONNECTIONS[connection_id]['client']
    else:
        raise ValueError(f'No active SSH Connection with id {connection_id}')


class Connection(BaseModel):
    id: str
    sys: str
    user: str
    start: datetime
    last_ts: datetime
    trash_dir: str

class ConnectionRequest(BaseModel):
    system: str
    user: str
    psw: str
    mfa: str
    restart: Union[bool, None] = False

@app.get("/list", response_model=List[Connection])
def list_jm():
    """Show initialized job managers"""
    out = []
    for c in CONNECTIONS.keys():
        conn = CONNECTIONS[c]
        out.append({a:conn[a] for a in conn if a != 'client'})
    return out


@app.post("/{connection_id}", response_model=Connection)
async def init(
    connection_id: str,
    req: ConnectionRequest
):
    global CONNECTIONS

    if connection_id not in CONNECTIONS.keys() or restart:
        try:
            logger.info(f"INIT - Initializing TACCJM {connection_id}.")
            client  = TACCSSHClient(
                req.system, user=req.user, psw=req.psw,
                mfa=req.mfa, working_dir=connection_id
            )
            logger.info(f"SUCCESS - TACCJM {connection_id} initialized successfully.")
        except ValueError as v:
            msg = f"Unable to initialize {connection_id} on {req.system} for {req.user}: {v}"
            raise HTTPException(status_code=404, detail=f"ssh_error: {msg}")
        except Exception as e:
            msg = f"Unable to initialize {connection_id} on {req.system} for {req.user}: {e}"
            raise HTTPException(status_code=401, detail=f"ssh_error: {msg}")
    else:
        raise HTTPException(status_code=409, detail=f"ssh_error: {msg}")

    ret = {
        "id": connection_id,
        "sys": client.system,
        "user": client.user,
        "start": datetime.now(),
        "last_ts": datetime.now(),
        "trash_dir": client.trash_dir,
    }
    CONNECTIONS[connection_id] = ret
    CONNECTIONS[connection_id]['client'] = client

    return ret


@app.get("/{connection_id}", response_model=Connection)
def get(
    connection_id: str,
):
    global CONNECTIONS


    if connection_id not in CONNECTIONS.keys():
      msg = f"Connection id {connnection_id} not found"
      raise HTTPException(status_code=404, detail=f"ssh_error: {msg}")
    else:
      ret = CONNECTIONS[connection_id]
      ret = {a:ret[a] for a in ret if a != 'client'}

      return ret


class CommandRequest(BaseModel):
    cmnd: str
    wait: Union[bool, None] = True


class ProcessRequest(BaseModel):
    cmnd_id: int
    nbytes: Union[int, None] = None
    wait: Union[bool, None] = True


class Command(BaseModel):
    id: int
    cmd: str
    ts: datetime
    status: str
    stdout: str
    stderr: str
    history: List[Dict]


@app.post("/{connection_id}/exec", response_model=Command)
def exec(connection_id: str, cmnd_req: CommandRequest):
    """Execute command"""
    ssh_client = _get_client(connection_id)
    logger.info(f"Executing new command on {connection_id}",
                extra={'command_request':cmnd_req})
    res = ssh_client.execute_command(cmnd_req.cmnd,
                                     wait=cmnd_req.wait,
                                     error=False)
    res = {i: res[i] for i in res if i != 'channel'}
    CONNECTIONS[connection_id]['last_ts'] = datetime.now()
    logger.info(f"Command {res['id']} executed on {connection_id}.",
                extra={'command_config':res})

    return res


@app.post("/{connection_id}/process", response_model=Command)
def process(connection_id: str, proc_req: ProcessRequest):
    """Process command"""

    ssh_client = _get_client(connection_id)
    logger.info(f"Processing command {proc_req.cmnd_id} on {connection_id}",
                extra={'process_request':proc_req})
    res = ssh_client.process_command(proc_req.cmnd_id,
                                     wait=proc_req.wait,
                                     error=False)
    res = {i: res[i] for i in res if i != 'channel'}
    CONNECTIONS[connection_id]['last_ts'] = datetime.now()
    logger.info(f"Command {res['id']} execute/processed on {connection_id}.",
                extra={'command_config':res})

    return res


class PathInfo(BaseModel):
    path: Union[str, None] = '.'
    filename: Union[str, None] = None
    st_gid: Union[int, None] = None
    st_mode: Union[int, None] = None
    st_mtime: Union[int, None] = None
    st_size: Union[int, None] = None
    st_uid: Union[int, None] = None
    ls_str: Union[str, None] = None


@app.get("/{connection_id}/files/{file_path:path}", response_model=List[PathInfo])
def list_files(connection_id:str, file_path: str):
    res = CONNECTIONS[connection_id].list_files(file_path)
    for r in res:
        r['path'] = file_path

    pdb.set_trace()

    return res


if __name__ == '__main__':
    import uvicorn

    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8000

    logger.info("Starting TACC SSH Server.")
    uvicorn.run(app, host=host, port=port)
    logger.info("TACC SSH Server started.")
