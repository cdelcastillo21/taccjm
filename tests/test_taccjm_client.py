"""
Tests for taccjm_client


"""
import os
import pdb
import psutil
import pytest
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm import taccjm_client as tc
from taccjm.taccjm_client import TACCJMError
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

# Port to srat test servers on
TEST_TACCJM_PORT = 8661
TEST_JM_ID = 'test-taccjm'

# TEST JM as configured
TEST_JM = {'jm_id': 'test-taccjm',
           'sys': 'stampede2.tacc.utexas.edu',
           'user': 'clos21',
           'apps_dir': '/scratch/06307/clos21/test-taccjm/apps',
           'jobs_dir': '/scratch/06307/clos21/test-taccjm/jobs'}
TEST_QUEUE = [{'job_id': '1111111', 'job_name': 'test-1',
              'username': 'clos21', 'state': 'Running',
              'nodes': '11', 'remaining': '11', 'start_time': '19:24:57'},
              {'job_id': '2222222', 'job_name': 'test-2', 'username': 'clos21',
               'state': 'Waiting', 'nodes': '16',
               'remaining': '16', 'start_time': '6:00:00'}]
TEST_ALLOCATIONS = [{'name': 'alloc-1',
                     'service_units': 1000,
                     'exp_date': '2022-12-31'},
                    {'name': 'alloc-2',
                     'service_units': 2222,
                     'exp_date': '2022-03-31'}]


def _init():
    """
    Initializes TEST_JM on server. Note if server not found, not server will
    be started if not found.

    """

    # Set port to test port
    tc.set_host(port=TEST_TACCJM_PORT)

    global TEST_JM
    if TEST_JM is None:
        mfa = input('TACC Token:')
        TEST_JM = tc.init_jm(TEST_JM_ID, SYSTEM, USER, PW, mfa)


def test_set_host():
    """
    Test set host

    Tests setting global host/port that the client looks for a taccjm server on.

    """
    # Set to something new
    tc.set_host(host='0.0.0.0', port=TEST_TACCJM_PORT+1)
    assert tc.TACCJM_PORT == TEST_TACCJM_PORT+1
    assert tc.TACCJM_HOST == '0.0.0.0'

    # Set back to default
    tc.set_host(host='localhost', port=TEST_TACCJM_PORT)
    assert tc.TACCJM_PORT == TEST_TACCJM_PORT
    assert tc.TACCJM_HOST == 'localhost'


def test_find_tjm_process():
    """
    Test find TACCJM Processes

    Tests searching and finding TACCJM processes
    """

    # Set to other port so don't kill JM if running other tests
    temp_port = TEST_TACCJM_PORT+1
    tc.set_host(port=temp_port)
    assert tc.TACCJM_PORT == temp_port

    # Search for processes, kill if we find them
    tc.find_tjm_processes(kill=True)

    # Now start the processes
    tc.find_tjm_processes(start=True)

    # Now start again (should do nothing)
    tc.find_tjm_processes(start=True)

    # Now kill them
    tc.find_tjm_processes(kill=True)

    # Set back to default
    tc.set_host(host='localhost', port=TEST_TACCJM_PORT)
    assert tc.TACCJM_PORT == TEST_TACCJM_PORT

def test_api_call():
    """
    Tests api_call method

    """
    # Set to other port so don't kill JM if running other tests
    temp_port = TEST_TACCJM_PORT+1
    tc.set_host(port=temp_port)
    assert tc.TACCJM_PORT == temp_port

    # Search for processes, kill if we find them
    tc.find_tjm_processes(kill=True)

    # Test good api call, but server is down, will have to retry
    tc.api_call('GET', 'list', data=None)

    # Test good api call, server is up so no restart
    tc.api_call('GET', 'list', data=None)

    # Test a bad endpoint
    with pytest.raises(TACCJMError):
        tc.api_call('GET', 'does/not/exist', data=None)

    # Set back to default
    tc.set_host(host='localhost', port=TEST_TACCJM_PORT)
    assert tc.TACCJM_PORT == TEST_TACCJM_PORT


@patch('taccjm.taccjm_client.api_call')
def test_list_jm(mock_api_call):
    """
    Tests operation of listing job managers available

    Note - All API calls are mocked
    """
    # Successful return with no JMs
    mock_api_call.return_value = []
    jms = tc.list_jms()
    assert jms==[]
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        jms = tc.list_jms()
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_init_jm(mock_api_call):
    """
    Tests operation of listing job managers available

    Note - All API calls are mocked
    """

    # Test a JM that has already been initialized (mocked)
    mock_api_call.return_value = ['already_init']
    with pytest.raises(ValueError):
        jm = tc.init_jm('already_init', 'foo', 'bar', 'test', 123456)
    mock_api_call.reset()

    # Successful return
    mock_api_call.return_value = TEST_JM
    jm = tc.init_jm(TEST_JM['jm_id'], TEST_JM['sys'],
                    TEST_JM['user'], 'testpw', 123456)
    assert type(jm) == dict
    assert jm == TEST_JM
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = [[], TACCJMError('Mock TACCJMError')]
    with pytest.raises(TACCJMError):
        jm = tc.init_jm(TEST_JM['jm_id'], TEST_JM['sys'],
                        TEST_JM['user'], 'testpw', 123456)
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_get_jm(mock_api_call):
    """
    Tests of getting info on a jm instance.

    Note - All API calls are mocked
    """

    # Successful return
    mock_api_call.return_value = TEST_JM
    jm = tc.get_jm(TEST_JM['jm_id'])
    assert jm == TEST_JM
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        jm = tc.get_jm(TEST_JM['jm_id'])
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_get_queue(mock_api_call):
    """
    Tests of getting job queue for system.

    Note - All API calls are mocked
    """

    # Successful return
    mock_api_call.return_value = TEST_QUEUE
    jm = tc.get_queue(TEST_JM['jm_id'])
    assert jm == TEST_QUEUE
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        jm = tc.get_queue(TEST_JM['jm_id'])
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_get_allocations(mock_api_call):
    """
    Tests of allocations on jm instance

    Note - All API calls are mocked
    """

    # Successful return
    mock_api_call.return_value = TEST_ALLOCATIONS
    jm = tc.get_allocations(TEST_JM['jm_id'])
    assert jm == TEST_ALLOCATIONS
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        jm = tc.get_allocations(TEST_JM['jm_id'])
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_get_files(mock_api_call):
    """
    Tests get files operation

    Note - All API calls are mocked
    """

    # Successful return
    mock_api_call.return_value = ["foo"]
    files = tc.list_files(TEST_JM['jm_id'])
    assert "foo" in files
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        jm = tc.list_files(TEST_JM['jm_id'])
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_peak_file(mock_api_call):
    """
    Tests peak file operation

    Note - All API calls are mocked
    """

    # Successful return
    text =  "Hello World\n"
    mock_api_call.return_value = text
    peak = tc.peak_file(TEST_JM['jm_id'], "foo")
    assert text==peak
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        jm = tc.peak_file(TEST_JM['jm_id'], "foo")
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_upload(mock_api_call):
    """
    Tests upload operation

    Note - All API calls are mocked
    """
    # Successful return
    mock_api_call.return_value = None
    tc.upload(TEST_JM['jm_id'], "foo", "foo", "*")
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        tc.upload(TEST_JM['jm_id'], "foo", "foo", "*")
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_download(mock_api_call):
    """
    Tests download operation

    Note - All API calls are mocked
    """
    # Successful return
    mock_api_call.return_value = "foo"
    tc.download(TEST_JM['jm_id'], "foo", "foo", "*")
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        tc.download(TEST_JM['jm_id'], "foo", "foo", "*")
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_remove(mock_api_call):
    """
    Tests remove operation

    Note - All API calls are mocked
    """
    # Successful return
    mock_api_call.return_value = None
    tc.remove(TEST_JM['jm_id'], "foo")
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        tc.remove(TEST_JM['jm_id'], "foo")
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_restore(mock_api_call):
    """
    Tests restore operation

    Note - All API calls are mocked
    """
    # Successful return
    mock_api_call.return_value = None
    tc.restore(TEST_JM['jm_id'], "foo")
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        tc.restore(TEST_JM['jm_id'], "foo")
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_write(mock_api_call):
    """
    Tests write operation

    Note - All API calls are mocked
    """
    # Successful return
    mock_api_call.return_value = None
    tc.write(TEST_JM['jm_id'], "foo", "foo.txt")
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        tc.write(TEST_JM['jm_id'], "foo", "foo.txt")
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_read(mock_api_call):
    """
    Tests read operation

    Note - All API calls are mocked
    """
    # Successful return
    mock_api_call.return_value = None
    tc.read(TEST_JM['jm_id'], "foo.txt", data_type="text")
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        tc.read(TEST_JM['jm_id'], "foo.txt", data_type="text")
    mock_api_call.reset()


@patch('taccjm.taccjm_client.api_call')
def test_list_apps(mock_api_call):
    """
    Tests list_apps operation

    Note - All API calls are mocked
    """
    # Successful return
    mock_api_call.return_value = ["foo"]
    tc.list_apps(TEST_JM['jm_id'])
    mock_api_call.reset()

    # Error
    mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
    with pytest.raises(TACCJMError):
        tc.list_apps(TEST_JM['jm_id'])
    mock_api_call.reset()

