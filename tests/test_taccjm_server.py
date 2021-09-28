"""
Integration tests for taccjm_server


"""
import os
import pdb
import hug
import pytest
import posixpath
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm import taccjm_server, TACCJobManager

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Note: .env file in tests directory must contain TACC_USER and TACC_PW variables defined
load_dotenv()

# Globals loaded from .env config file in tests directory
global SYSTEM, USER, PW, SYSTEM, ALLOCATION
USER = os.environ.get("TACCJM_USER")
PW = os.environ.get("TACCJM_PW")
SYSTEM = os.environ.get("TACCJM_SYSTEM")
ALLOCATION = os.environ.get("TACCJM_ALLOCATION")

# JM we will use for testing
test_jm = f"test_{SYSTEM}"

print(os.getcwd())

# Initialized Flag
initialized = False
def _init(mfa):
    global initialized
    if not initialized:
        init_args = {'jm_id': test_jm, 'system': SYSTEM, 'user': USER, 'psw': PW, 'mfa':mfa}
        response = hug.test.post(taccjm_server, 'init', init_args)
        initialized = True


def test_jms(mfa):
    """
    Test managing JM instances and errors
    """
    init_args = {'jm_id': test_jm, 'system': SYSTEM, 'user': USER, 'psw': PW, 'mfa':mfa}

    # List JMs - There should be none
    response = hug.test.get(taccjm_server, 'list', None)
    assert response.status == '200 OK'
    assert response.data == []

    # Get JM before added - error
    response = hug.test.get(taccjm_server, f"{test_jm}", None)
    assert response.status == '404 Not Found'
    assert response.data['errors'] == {'jm_error': f"TACCJM {test_jm} does not exist."}

    # Initialize JM
    response = hug.test.post(taccjm_server, 'init', init_args)
    assert response.status == '200 OK'
    assert response.data['jm_id'] == f"test_{SYSTEM}"
    assert response.data['sys'] == f"{SYSTEM}.tacc.utexas.edu"
    assert response.data['user'] == USER

    # Try initializing JM with same jm_id -> Should give error
    response = hug.test.post(taccjm_server, 'init', init_args)
    assert response.status == '409 Conflict'
    assert response.data['errors'] == {'jm_error': f"TACCJM {test_jm} already exists."}

    # Get JM just added
    response = hug.test.get(taccjm_server, f"{test_jm}", None)

    # Initialize JM again, but with bad user, should get Unauthorized error due to authentication
    bad_init_args = {'jm_id': 'bad_jm', 'system': SYSTEM, 'user': 'foo', 'psw': PW, 'mfa':mfa}
    response = hug.test.post(taccjm_server, 'init', bad_init_args)
    assert response.status == '401 Unauthorized'
    assert response.data['errors']['jm_error'].startswith('Unable to initialize TACCJM')

    # Initialize JM again, but with bad system, should get Not Found error
    bad_init_args = {'jm_id': 'bad_jm', 'system': 'foo', 'user': USER, 'psw': PW, 'mfa':mfa}
    response = hug.test.post(taccjm_server, 'init', bad_init_args)
    assert response.status == '404 Not Found'
    assert response.data['errors']['jm_error'].startswith('Unable to initialize TACCJM')

    # Test getting queue on initialized JM
    response = hug.test.get(taccjm_server, f"{test_jm}/queue", {'user': 'all'})
    assert response.status == '200 OK'

    # Test getting allocations on initialized JM
    response = hug.test.get(taccjm_server, f"{test_jm}/allocations", None)
    assert response.status == '200 OK'

def test_files(mfa):
    """Test file operations"""

    _init(mfa)

    # Upload a file, this script for example
    response = hug.test.put(taccjm_server, f"{test_jm}/files/upload",
            {'local_path':'./tests/test_taccjm_server.py', 'remote_path':'test_file'})
    assert response.status == '200 OK'

    # Get files on home directory and make sure just uploaded is there.
    response = hug.test.get(taccjm_server, f"{test_jm}/files/list", None)
    assert response.status == '200 OK'
    assert 'test_file' in response.data

    # Get files on invalid directory
    response = hug.test.get(taccjm_server, f"{test_jm}/files/list", {'path':'does-not-exist'})
    assert response.status == '404 Not Found'

    # Remove file just uploaded
    response = hug.test.delete(taccjm_server, f"{test_jm}/files/remove",
            {'remote_path':'test_file'})
    assert response.status == '200 OK'
    response = hug.test.get(taccjm_server, f"{test_jm}/files/list", None)
    assert 'test_file' not in response.data

    # Restore file just removed
    response = hug.test.put(taccjm_server, f"{test_jm}/files/restore",
            {'remote_path':'test_file'})
    assert response.status == '200 OK'

    # Now download file sent
    response = hug.test.get(taccjm_server, f"{test_jm}/files/download",
            {'remote_path':'test_file', 'local_path':'./tests/test_download'})
    assert response.status == '200 OK'
    assert 'test_download' in os.listdir('tests')
    os.remove('tests/test_download')


def test_data(mfa):
    """Test file operations"""

    _init(mfa)

    # Send local data to a file
    response = hug.test.put(taccjm_server, f"{test_jm}/data/send",
            {'data':'foo', 'remote_path':'test_data'})
    assert response.status == '200 OK'
    response = hug.test.get(taccjm_server, f"{test_jm}/files/list")
    assert 'test_data' in response.data

    # Send data errors
    response = hug.test.put(taccjm_server, f"{test_jm}/data/send",
            {'data':['foo'], 'remote_path':'test_data'})
    assert response.status == '400 Bad Request'
    response = hug.test.put(taccjm_server, f"{test_jm}/data/send",
            {'data':'foo', 'remote_path':'foo/bar/test_data'})
    assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'send_data',
            side_effect=PermissionError('Mock unexpected error.')):
        response = hug.test.put(taccjm_server, f"{test_jm}/data/send",
                {'data':'foo', 'remote_path':'/test_data'})
        assert response.status == '403 Forbidden'
    with patch.object(TACCJobManager, 'send_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.put(taccjm_server, f"{test_jm}/data/send",
            {'data':'foo', 'remote_path':'test'})
        assert response.status == '500 Internal Server Error'

    # Get data we just sent
    response = hug.test.get(taccjm_server, f"{test_jm}/data/receive",
            {'remote_path':'test_data', 'data_type':'text'})
    assert response.status == '200 OK'
    assert response.data=='foo'

    # Receive data errors
    response = hug.test.get(taccjm_server, f"{test_jm}/data/receive",
        {'remote_path':'test_data', 'data_type':'bad-type'})
    assert response.status == '400 Bad Request'
    response = hug.test.get(taccjm_server, f"{test_jm}/data/receive",
        {'remote_path':'foo/bar/test_data', 'data_type':'text'})
    assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'get_data',
            side_effect=PermissionError('Mock permission error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/data/receive",
            {'remote_path':'/test_data', 'data_type':'text'})
        assert response.status == '403 Forbidden'
    with patch.object(TACCJobManager, 'get_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/data/receive",
            {'remote_path':'test_data', 'data_type':'text'})
        assert response.status == '500 Internal Server Error'








