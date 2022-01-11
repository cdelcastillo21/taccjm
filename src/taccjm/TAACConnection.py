""" TACC Connection Class


"""

import os                       # OS system utility functions for local system
import posixpath                # Paths on remote system (assumed UNIX)
import errno                    # For error messages
import tarfile                  # For sending compressed directories
import re                       # Regular Expressions
import pdb                      # Debug
import json                     # For saving and loading job configs to disk
import time                     # Time functions
import logging                  # Used to setup the Paramiko log file
import stat                     # For reading file stat codes
from datetime import datetime   # Date time functionality
from fnmatch import fnmatch     # For unix-style filename pattern matching
from taccjm.utils import *      # TACCJM Util functions for config files/dicts
from taccjm.constants import *  # For application configs
from typing import Tuple, List  # Type hints
from typing import Union        # Type hints

# Modified paramiko ssh client and common paramiko exceptions
from taccjm.SSHClient2FA import SSHClient2FA
from paramiko import SSHException, AuthenticationException, BadHostKeyException

# Custom exception for handling remote command errors
from taccjm.exceptions import TJMCommandError

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


logger = logging.getLogger(__name__)


class TACCConnection():
    """
    Class defining an ssh connection to a TACC system .

    Attributes
    ----------
    system : str
        Name of remote system to connect to. Supported systems: stampede2,
        ls5, frontera, maverick2
    user : str
        Name of tacc user connecting via ssh to resource.
    jobs_dir : str
        Unix-stlye path to directory to store jobs.
    apps_dir : str
        Unix-style path to directory to store applications.
    scripts_dir : str
        Unix-stye path to directory where scripts for job manager can be found.
    trash_dir : str
        Unix-stye path to directory where unwanted files/folders go.

    Notes
    -----
    All remote paths should use Unix path seperateor '/'.

    """

    ROOT = 'tacc.utexas.edu'
    SYSTEMS = ['stampede2', 'ls6', 'frontera', 'maverick2']
    USER_PROMPT = "Username:"
    PSW_PROMPT = "Password:"
    MFA_PROMPT ="TACC Token Code:"
    SCRATCH_DIR = "$SCRATCH"


    def __init__(self, system, user=None,
            psw=None, mfa=None, working_dir='taccjm'):
        """
        Initialize Job Manager connection and directories.

        Parameters
        ----------
        system : str
            Name of system to connect to via ssh. Supported systems:
            stampede2, ls5, frontera, maverick2
        user : str , optional
            Name of user connecting. If non provided input prompt will appear.
        psw : str , optional
            Password for user connecting. Secure prompt will appear if no
            password is provided.
        mfa : str , optional
            2-Factor Authenticaion token for user.. If non provided
            input prompt will appear
        working_dir: str , default='taccjm'
            Unix-style path relative to user's SCRATCH directory to place all
            data related to this job manager instance. This includes the apps,
            jobs, scripts, and trash directories.
        """

        if system not in self.SYSTEMS:
            m = f"Unrecognized system {system}. Options - {self.SYSTEMS}."
            logger.error(m)
            raise ValueError(m)
        if any([working_dir.startswith('../'),
                working_dir.endswith('/..'),
                '/../' in working_dir ]):
            msg = f"Inavlid working dir {working_dir} - Contains '..'."
            logger.error(msg)
            raise ValueError(msg)

        # Connect to system
        logger.info(f"Connecting {user} to system {system}...")
        self.system= f"{system}.tacc.utexas.edu"
        self._client = SSHClient2FA(user_prompt=self.USER_PROMPT,
                psw_prompt=self.PSW_PROMPT,
                mfa_prompt=self.MFA_PROMPT)
        self.user = self._client.connect(self.system,
                uid=user, pswd=psw, mfa_pswd=mfa)
        logger.info(f"Succesfuly connected to {system}")

        # Get taccjm working directory, relative to users scratch directory
        logger.info("Resolving path ${self.SCRATCH_DIR} for user {self.user}")
        self.SCRATCH_DIR = scratch_dir = self._execute_command(
                f"echo {self.SCRATCH_DIR}").strip()
        taccjm_dir = posixpath.join(self.SCRATCH_DIR, working_dir)

        # Initialze jobs, apps, scripts, and trash dirs
        logger.info("Creating if jobs, apps, scripts, and trash dirs")
        for d in zip(['jobs_dir', 'apps_dir', 'scripts_dir', 'trash_dir'],
                      ['jobs', 'apps', 'scripts', 'trash']):
            setattr(self, d[0], posixpath.join(taccjm_dir,d[1]))
            self._mkdir(getattr(self, d[0]), parents=True)

        # Get python path and home dir
        self.python_path = self._execute_command('which python')


    def execute_command(self, cmnd) -> None:
        """
        Executes a shell command through ssh on a TACC resource.

        Parameters
        ----------
        cmnd : str
            Command to execute. Be careful! rm commands and such will delete
            things permenantly!

        Returns
        -------
        out : str
            stdout return from command.

        Raises
        ------
        TJMCommandError
            If command executed on TACC resource returns non-zero return code.
        """
        try:
            stdin, stdout, stderr = self._client.exec_command(cmnd)
        except SSHException as ssh_error:
            # Will only occur if ssh connection is broken
            msg = "TACCJM ssh connection error: {ssh_error.__str__()}"
            logger.error(msg)
            raise ssh_error

        out = stdout.read().decode('utf-8')
        err = stderr.read().decode('utf-8')
        rc = stdout.channel.recv_exit_status()

        if rc!=0:
            # Build base TJMCommand Error, only place this should be done
            t = TJMCommandError(self.system, self.user, cmnd, rc, out, err)

            # Only log the actual TJMCommandError object once, here
            logger.error(t.__str__())

            raise t

        return out


    def _mkdir(self, path, parents=False) -> None:
        """
        Creates directory on remote system.

        Parameters
        ----------
        path: str
            Unix-stype path on remote system to create.

        Returns
        -------
        err_code : int
            An SFTP error code int like SFTP_OK (0).

        """
        cmnd = f"mkdir {path}" if not parents else f"mkdir -p {path}"

        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "_mkdir - Could not create directory"
            logger.error(tjm_error.message)
            raise tjm_error
