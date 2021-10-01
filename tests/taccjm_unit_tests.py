"""
Unit Tests for TACC JobManager Class

Calls and response to remote TACC resources are mocked in these tests to check for functionality 


Note:


References:

"""
import os
import pdb
import pytest
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm.TACCJobManager import TACCJobManager

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def mock_init_jm()
    if system not in self.TACC_SYSTEMS:
        msg = f"Unrecognized TACC system {system}. Must be one of {self.TACC_SYSTEMS}."
        logger.error(msg)
        raise ValueError(msg)
    if any([working_dir.startswith('../'),
            working_dir.endswith('/..'),
            '/../' in working_dir ]):
        msg = f"Inavlid working directory {working_dir}. Contains '..' which isn't allowed."
        logger.error(msg)
        raise ValueError(msg)

    # Connect to system
    logger.info(f"Connecting {user} to TACC system {system}...")
    self.system= f"{system}.tacc.utexas.edu"
    self._client = SSHClient2FA(user_prompt=self.TACC_USER_PROMPT,
            psw_prompt=self.TACC_PSW_PROMPT,
            mfa_prompt=self.TACC_MFA_PROMPT)
    self._client.load_system_host_keys()
    self.user = self._client.connect(self.system, uid=user, pswd=psw, mfa_pswd=mfa)
    logger.info(f"Succesfuly connected to {system}")

    # Get taccjm working directory, relative to users scratch directory
    logger.info("Resolving SCRATCH directory path ${self.SCRATCH_DIR} for user {self.user}")
    self.scratch_dir = scratch_dir = self._execute_command(f"echo {self.SCRATCH_DIR}").strip()
    taccjm_dir = posixpath.join(scratch_dir, working_dir)

    # Initialze jobs, apps, scripts, and trash dirs
    logger.info("Creating if jobs, apps, sripts, and trash dirs if they don't already exist")
    for d in zip(['jobs_dir', 'apps_dir', 'scripts_dir', 'trash_dir'],
                  ['jobs', 'apps', 'scripts', 'trash']):
        setattr(self, d[0], posixpath.join(taccjm_dir,d[1]))
        # Skipping making directories on system
    #    self._mkdir(getattr(self, d[0]), parents=True)

def test_queue():
    """get_queue unit tests
    """
    with open('./tests/test_app/empty_queue.txt', 'r') as f:
        empty_queue = f.read()
        with patch.object(TACCJobManager, '_execute_command',
                return=empty_queue):

    with open('./tests/test_app/full_queue.txt', 'r') as f:
        full_queue = f.read()

