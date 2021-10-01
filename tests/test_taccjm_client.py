"""
Integration tests for taccjm_client


"""
import os
import pdb
import psutil
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm import taccjm_client as tc
from taccjm.utils import *

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Note: .env file in tests directory must contain
#   - TACC_USER
#   - TACC_PW 
#   - TACC_SYSTEM
#   - TACC_ALLOCATION
load_dotenv()

# Globals loaded from .env config file in tests directory
# Used for connecting to TACC system for integration tests
global SYSTEM, USER, PW, SYSTEM, ALLOCATION
USER = os.environ.get("TACCJM_USER")
PW = os.environ.get("TACCJM_PW")
SYSTEM = os.environ.get("TACCJM_SYSTEM")
ALLOCATION = os.environ.get("TACCJM_ALLOCATION")

# Global Initialized Flag - Marks if JM has been initailized for any test
initialized = False

# Port to srat test servers on
TEST_TACCJM_PORT=8661

# def _init(mfa):
#
#     # Set port to test port
#     set_host(port=TEST_TACCJM_PORT)
#
#     global initialized
#     if not initialized:
#         init_args = {'jm_id': test_jm, 'system': SYSTEM,
#                      'user': USER, 'psw': PW, 'mfa':mfa}
#         response = hug.test.post(taccjm_server, 'init', init_args)
#         initialized = True

def test_set_host():
    """
    Test set host

    Tests setting global host/port that the client looks for a taccjm server on.

    """
    # Set to something new
    tc.set_host(host='0.0.0.0', port=8662)
    assert tc.TACCJM_PORT == 8662
    assert tc.TACCJM_HOST == '0.0.0.0'

    # Set back to default
    tc.set_host(host='localhost', port=TEST_TACCJM_PORT)
    assert tc.TACCJM_PORT == 8661
    assert tc.TACCJM_HOST == 'localhost'


def test_find_tjm_process():
    """
    Test find TACCJM Processes

    Tests searching and finding TACCJM processes
    """

    processes = [{'name':'hb', 'pid':11111,
        'cmdline': tc._parse_hb_cmd().split()},
                 {'name':'server', 'pid':11111,
                     'cmdline': tc._parse_srv_cmd().split()}]
    with patch(tc.psutil.process_iter, return_value=processes):
        proc = tc.find_tjm_processes()
        proc
        pdb.set_trace()



