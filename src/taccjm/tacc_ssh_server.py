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


class Connection(BaseModel):
    id: str
    sys: str
    user: str
    start: datetime
    up_time: float
    trash_dir: str

class ConnectionRequest(BaseModel):
    system: str
    user: str
    psw: str
    mfa: str
    restart: Union[bool, None] = False

@app.get("/list")
def list_jm():
    """Show initialized job managers"""
    out = []
    for c in CONNECTIONS.keys():
        out.append(
            {
                "name": c,
                "sys": CONNECTIONS[c].system,
                "user": CONNECTIONS[c].user,
                "trash_dir": CONNECTIONS[c].trash_dir,
            }
        )
    return out


@app.post("/{connection_id}", response_model=Connection)
def init(
    connection_id: str,
    req: ConnectionRequest
):
    global CONNECTIONS

    if connection_id not in CONNECTIONS.keys() or restart:
        try:
            logger.info(f"INIT - Initializing TACCJM {connection_id}.")
            CONNECTIONS[connection_id] = TACCSSHClient(
                req.system, user=req.user, psw=req.psw, mfa=req.mfa, working_dir=connection_id
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
        "sys": CONNECTIONS[connection_id].system,
        "user": CONNECTIONS[connection_id].user,
        "start": datetime.now(),
        "up_time": 0.0,
        "trash_dir": CONNECTIONS[connection_id].trash_dir,
    }

    return ret


class CommandRequest(BaseModel):
    cmnd: Union[str, int]
    nbytes: Union[int, None] = None
    wait: Union[bool, None] = True
    error: Union[bool, None] = True


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
    if connection_id not in CONNECTIONS.keys():
        raise ValueError(f'Invalid connection ID {connection_id}')
    res = CONNECTIONS[connection_id].execute_command(cmnd_req.cmnd,
                                                     wait=cmnd_req.wait,
                                                     error=cmnd_req.error)
    res = {i: res[i] for i in res if i != 'channel'}

    return res


@app.post("/{connection_id}/process", response_model=Command)
def process(connection_id: str, req: CommandRequest):
    res = CONNECTIONS[connection_id].process_command(req.id,
                                                     nbytes=req.nbytes,
                                                     wait=req.wait,
                                                     error=req.error)
    res = {i: res[i] for i in res if i != 'channel'}

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
