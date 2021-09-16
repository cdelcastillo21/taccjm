
import os
import pdb
import hug
import pytest
import posixpath
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm.TACCJobManager import TACCJobManager, TJMCommandError
from taccjm.SSHClient2FA import SSHClient2FA
from paramiko import SSHException, AuthenticationException, BadHostKeyException

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Note: .env file in tests directory must contain TACC_USER and TACC_PW variables defined
load_dotenv()

global SYSTEM, USER, PW, SYSTEM, ALLOCATION
USER = os.environ.get("TACCJM_USER")
PW = os.environ.get("TACCJM_PW")
SYSTEM = os.environ.get("TACCJM_SYSTEM")
ALLOCATION = os.environ.get("TACCJM_ALLOCATION")


test
