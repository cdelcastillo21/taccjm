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
from typing import Union, List, Tuple
from taccjm.TACCConnection import TACCConnection, TJMCommandError

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Initialize server logger
logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)

# Dictionary containing all job manager instances being managed
# Note there could be multiple instance if managing more than one system
CONN = {}


@hug.exception(ValueError)
def handle_custom_exceptions(exception):
    """Handling exceptions when ValueError due to input thrown."""

    # Raise 400 Bad Request if invalid data type passed
    err = falcon.HTTPError(falcon.HTTP_400, "BadRequest", str(exception))
    logger.error(str(err))
    raise err


@hug.exception(FileNotFoundError)
def handle_custom_exceptions(exception):
    """Handling exceptions when resource can't be found."""

    # Raise 404 not found error if local or remoate path don't exist
    err = falcon.HTTPError(falcon.HTTP_404, "NotFound", str(exception))
    logger.error(str(err))
    raise err


@hug.exception(PermissionError)
def handle_custom_exceptions(exception):
    """Handling exception when don't have access to resource"""

    # Raise 403 forbidden if dont have permissions to access either paath
    err = falcon.HTTPError(falcon.HTTP_403, "Forbidden", str(exception))
    logger.error(str(err))
    raise err


@hug.exception(TJMCommandError)
def handle_custom_exceptions(exception):
    """Handling other command errors"""

    # Log TCJMCommandError message
    logger.error(f"{str(exception)}")

    headers = {}
    # Add extra to HTTP error
    attrs = ['system', 'user', 'command' ,'rc', 'stderr', 'stdout']
    for a in attrs:
        headers[a] = exception.__getattribute__(a)

    # Raise 500 internal server error for unanticipated error
    err = falcon.HTTPError(falcon.HTTP_500, "Command error", str(exception),
            headers)
    logger.error(str(err))
    raise err


def _check_init(cid):
    """Check if TACC Connection is initalized"""
    if cid not in CONN.keys():
        raise falcon.HTTPError(falcon.HTTP_404,
                "error", f"TACC Connection {cid} does not exist.")


@hug.post('/init')
def init_jm(cid:str, system:str, user:str, psw:str, mfa:str):
    """Initialize TACC Connection Instances"""
    global JM

    if jm_id not in JM.keys():
        try:
            logger.info(f"INIT - Initializing TACC Connection {cid}.")
            CONN[id] = TACCJobManager(system, user=user, psw=psw, mfa=mfa,
                    working_dir=cid)
            logger.info(f"SUCCESS - TACC Connection {cid} initialized.")

            ret = {'cid':cid,
                   'sys':CONN[jm_id].system,
                   'user':CONN[jm_id].user,
                   'apps_dir':CONN[jm_id].apps_dir,
                   'jobs_dir':CONN[jm_id].jobs_dir,
                   'scripts_dir':CONN[jm_id].scripts_dir,
                   'trash_dir':CONN[jm_id].trash_dir}
            return ret
        except ValueError as v:
            # Raise Not Found HTTP code for non TACC system
            msg = f"Unable to initialize {cid} on {system} for {user}: {v}"
            raise falcon.HTTPError(falcon.HTTP_404, "error", msg)
        except Exception as e:
            # Raise Unauthorized HTTP code for bad login to system
            msg = f"Unable to initialize {cid} on {system} for {user}: {e}"
            raise falcon.HTTPError(falcon.HTTP_401, "error", msg)
    else:
        # Raise Conflict HTTP error
        raise falcon.HTTPError(falcon.HTTP_409,
                "error", f"TACC Connection {cid} already exists.")


@hug.get('/list')
def ls():
    """Show initialized tacc connections"""
    out = []
    for c in CONN.keys():
        out.append({'cid':c, 'sys':CONN[c].system, 'user':CONN[c].user,
            'apps_dir':CONN[c].apps_dir, 'jobs_dir':CONN[c].jobs_dir})
    return out


@hug.get('/{cid}')
def get_jm(cid:str):
    """Get Job Manager managed by this server if it exists"""

    _check_init(cid)

    jm = {'cid':cid, 'sys':CONN[cid].system, 'user':CONN[cid].user,
          'apps_dir':CONN[cid].apps_dir, 'jobs_dir':CONN[cid].jobs_dir,
          'trash_dir':CONN[cid].trash_dir, 'scripts_dir':CONN[cid].scripts_dir}
    return jm


@hug.get('/{cid}/execute')
def execute_command(cid:str, cmd:str):
    """Execute command on tacc connection."""

    _check_init(cid)

    return CONN[cid].showq(user=user)


