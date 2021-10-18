"""
Integration tests for taccjm_server


"""
import os
import pdb
import hug
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm import taccjm_server, TACCJobManager
from taccjm.TACCJobManager import TJMCommandError
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

# JM we will use for testing, only initailize once
test_jm = f"test_{SYSTEM}"

# Global Initialized Flag - Marks if JM has been initailized for any test
initialized = False


def _init():
    """Call once to initailize JM for tests"""
    global initialized
    if not initialized:
        mfa = input("\nTACC Token:")
        init_args = {'jm_id': test_jm, 'system': SYSTEM,
                     'user': USER, 'psw': PW, 'mfa':mfa}
        response = hug.test.post(taccjm_server, 'init', init_args)
        initialized = True


def test_jms(mfa):
    """
    Test managing JM instances and errors
    """
    global intiailized

    init_args = {'jm_id': test_jm, 'system': SYSTEM,
                 'user': USER, 'psw': PW, 'mfa':mfa}

    # List JMs - There should be none
    response = hug.test.get(taccjm_server, 'list', None)
    assert response.status == '200 OK'
    assert response.data == []

    # Get JM before added - error
    error = {'jm_error': f"TACCJM {test_jm} does not exist."}
    response = hug.test.get(taccjm_server, f"{test_jm}", None)
    assert response.status == '404 Not Found'
    assert response.data['errors'] == error

    # Initialize JM
    _init()

    # Try initializing JM with same jm_id -> Should give error
    error = {'jm_error': f"TACCJM {test_jm} already exists."}
    response = hug.test.post(taccjm_server, 'init', init_args)
    assert response.status == '409 Conflict'
    assert response.data['errors'] == error

    # List JMs, JM added should be there
    response = hug.test.get(taccjm_server, 'list', None)
    assert response.status == '200 OK'
    assert len(response.data) == 1
    assert response.data[0]['jm_id'] == test_jm

    # Get JM just added
    response = hug.test.get(taccjm_server, f"{test_jm}", None)

    # Initialize JM again, but with bad user, should get Unauthorized error
    bad_init_args = {'jm_id': 'bad_jm', 'system': SYSTEM,
                     'user': 'foo', 'psw': PW, 'mfa':mfa}
    response = hug.test.post(taccjm_server, 'init', bad_init_args)
    assert response.status == '401 Unauthorized'

    # Initialize JM again, but with bad system, should get Not Found error
    bad_init_args = {'jm_id': 'bad_jm', 'system': 'foo',
                     'user': USER, 'psw': PW, 'mfa':mfa}
    response = hug.test.post(taccjm_server, 'init', bad_init_args)
    assert response.status == '404 Not Found'

    # Test getting queue on initialized JM
    response = hug.test.get(taccjm_server, f"{test_jm}/queue", {'user': 'all'})
    assert response.status == '200 OK'

    # Test getting allocations on initialized JM
    response = hug.test.get(taccjm_server, f"{test_jm}/allocations", None)
    assert response.status == '200 OK'


def test_files():
    """Test file operations"""

    _init()

    # Upload a file, this script for example
    response = hug.test.put(taccjm_server, f"{test_jm}/files/upload",
            {'local_path':'./tests/test_taccjm_server.py',
             'remote_path':'test_file'})
    assert response.status == '200 OK'

    # Get files on home directory and make sure just uploaded is there.
    response = hug.test.get(taccjm_server, f"{test_jm}/files/list")
    assert response.status == '200 OK'

    # Get files on invalid directory
    response = hug.test.get(taccjm_server,
            f"{test_jm}/files/list", {'path':'does-not-exist'})
    assert response.status == '404 Not Found'
    pdb.set_trace()

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


def test_read_write(mfa):
    """Test read and write file operations"""

    _init(mfa)

    # Send local data to a file
    response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
            {'data':'foo', 'remote_path':'test_data'})
    assert response.status == '200 OK'
    response = hug.test.get(taccjm_server, f"{test_jm}/files/list")
    assert 'test_data' in response.data

    # Send data errors
    response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
            {'data':['foo'], 'remote_path':'test_data'})
    assert response.status == '400 Bad Request'
    response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
            {'data':'foo', 'remote_path':'foo/bar/test_data'})
    assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'send_data',
            side_effect=PermissionError('Mock unexpected error.')):
        response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
                {'data':'foo', 'remote_path':'/test_data'})
        assert response.status == '403 Forbidden'
    with patch.object(TACCJobManager, 'send_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
            {'data':'foo', 'remote_path':'test'})
        assert response.status == '500 Internal Server Error'

    # Get data we just sent
    response = hug.test.get(taccjm_server, f"{test_jm}/files/read",
            {'remote_path':'test_data', 'data_type':'text'})
    assert response.status == '200 OK'
    assert response.data=='foo'

    # read data errors
    response = hug.test.get(taccjm_server, f"{test_jm}/files/read",
        {'remote_path':'test_data', 'data_type':'bad-type'})
    assert response.status == '400 Bad Request'
    response = hug.test.get(taccjm_server, f"{test_jm}/files/read",
        {'remote_path':'foo/bar/test_data', 'data_type':'text'})
    assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'get_data',
            side_effect=PermissionError('Mock permission error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/files/read",
            {'remote_path':'/test_data', 'data_type':'text'})
        assert response.status == '403 Forbidden'
    with patch.object(TACCJobManager, 'get_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/files/read",
            {'remote_path':'test_data', 'data_type':'text'})
        assert response.status == '500 Internal Server Error'

    # peak at file
    response = hug.test.get(taccjm_server, f"{test_jm}/files/peak",
            {'path':'test_data'})
    assert response.status == '200 OK'
    assert response.data=='foo'

    # peak file errors
    response = hug.test.get(taccjm_server, f"{test_jm}/files/peak",
        {'path':'foo/bar/test_data'})
    assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'peak_file',
            side_effect=PermissionError('Mock permission error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/files/peak",
            {'path':'foo/bar/test_data'})
        assert response.status == '403 Forbidden'


def test_apps(mfa):
    """Test app operations"""

    _init(mfa)

    # Deploy an app
    app_config = load_templated_json_file('tests/test_app/app.json',
            'tests/test_app/project.ini')
    response = hug.test.post(taccjm_server, f"{test_jm}/apps/deploy",
            {'app_config':app_config, 'local_app_dir':'./tests/test_app',
                'overwrite':True})
    assert response.status == '200 OK'
    app_id = response.data['name']

    # Assert App just deployed exists in list of apps
    response = hug.test.get(taccjm_server, f"{test_jm}/apps/list")
    assert response.status == '200 OK'
    assert app_id in response.data

    # Get App Config back
    response = hug.test.get(taccjm_server, f"{test_jm}/apps/{app_id}")
    assert response.status == '200 OK'
    assert response.data['name']==app_id

    # Mock deploy errors
    with patch.object(TACCJobManager, 'deploy_app',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.post(taccjm_server, f"{test_jm}/apps/deploy",
            {'app_config':app_config})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'deploy_app',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.post(taccjm_server, f"{test_jm}/apps/deploy",
            {'app_config':app_config})
        assert response.status == '400 Bad Request'
    with patch.object(TACCJobManager, 'deploy_app',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.post(taccjm_server, f"{test_jm}/apps/deploy",
            {'app_config':app_config})
        assert response.status == '500 Internal Server Error'

    # Mock get errors
    with patch.object(TACCJobManager, 'get_app',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/apps/test_app",
            {'app_config':app_config})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'get_app',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/apps/test_app",
            {'app_config':app_config})
        assert response.status == '500 Internal Server Error'


def test_jobs(mfa):
    """Test job operations"""

    _init(mfa)

    # Deploy test app
    app_config = load_templated_json_file('tests/test_app/app.json',
            'tests/test_app/project.ini')
    response = hug.test.post(taccjm_server, f"{test_jm}/apps/deploy",
            {'app_config':app_config, 'local_app_dir':'./tests/test_app',
                'overwrite':True})
    assert response.status == '200 OK'
    app_id = response.data['name']

    # Set-up test job using app
    job_config = load_templated_json_file('tests/test_app/job.json',
            'tests/test_app/project.ini')
    job_config['allocation']=ALLOCATION
    response = hug.test.post(taccjm_server, f"{test_jm}/jobs/deploy",
            {'job_config':job_config})
    assert response.status == '200 OK'
    job_id = response.data['job_id']

    # Check for job in list of jobs
    response = hug.test.get(taccjm_server, f"{test_jm}/jobs/list",{})
    assert response.status == '200 OK'
    assert job_id in response.data

    # Write some data to a job file
    response = hug.test.put(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/write",
            {'data':'hello world\n', 'path':'hello.txt'})
    assert response.status == '200 OK'

    # Read data from file just written
    response = hug.test.get(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/read",
            {'path':'hello.txt', 'data_type':'text'})
    assert response.status == '200 OK'
    assert response.data == 'hello world\n'

    # Download file just written
    response = hug.test.get(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/download",
            {'path':'hello.txt'})
    assert response.status == '200 OK'
    with open(os.path.join(job_id,'hello.txt'), 'r') as f:
        assert f.read() == 'hello world\n'

    # Rename it and Upload it back to job directory
    with open(os.path.join(job_id,'goodbye.txt'), 'w') as f:
        f.write('goodby world\n')
    response = hug.test.put(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/upload",
            {'path':os.path.join(job_id,'goodbye.txt')})
    assert response.status == '200 OK'

    # Check file uploaded is in job directory
    response = hug.test.get(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/list",
            {})
    assert response.status == '200 OK'
    assert 'goodbye.txt' in response.data

    # Peak at file just uploaded
    response = hug.test.get(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/peak",
            {'path':'goodbye.txt'})
    assert response.status == '200 OK'
    assert response.data == 'goodby world\n'

    # Cleanup files sent/received
    os.system(f"rm -rf {job_id}")

    # Submit job
    response = hug.test.put(taccjm_server,
            f"{test_jm}/jobs/{job_id}/submit",{})
    assert response.status == '200 OK'
    assert 'slurm_id' in response.data.keys()
    slurm_id = response.data['slurm_id']

    # Cancel job
    response = hug.test.put(taccjm_server,
            f"{test_jm}/jobs/{job_id}/cancel",{})
    assert response.status == '200 OK'
    assert 'slurm_id' not in response.data.keys()
    assert slurm_id in response.data['slurm_hist']

    # Mock get_job errors
    with patch.object(TACCJobManager, 'get_job',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/jobs/{job_id}",{})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'get_job',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/jobs/{job_id}",{})
        assert response.status == '500 Internal Server Error'

    # Mock deploy_job errors
    with patch.object(TACCJobManager, 'setup_job',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.post(taccjm_server, f"{test_jm}/jobs/deploy",
                {'job_config':job_config})
        assert response.status == '400 Bad Request'
    with patch.object(TACCJobManager, 'setup_job',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.post(taccjm_server, f"{test_jm}/jobs/deploy",
                {'job_config':job_config})
        assert response.status == '500 Internal Server Error'

    # Mock list job files errors
    with patch.object(TACCJobManager, 'ls_job',
            side_effect=TJMCommandError('','','',1,'','','')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/list",
                {})
        assert response.status == '404 Not Found'

    # Mock write job data errors
    with patch.object(TACCJobManager, 'send_job_data',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/write",
                {'path':'test.txt', 'data':'foo'})
        assert response.status == '400 Bad Request'
    with patch.object(TACCJobManager, 'send_job_data',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/write",
                {'path':'test.txt', 'data':'foo'})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'send_job_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/write",
                {'path':'test.txt', 'data':'foo'})
        assert response.status == '500 Internal Server Error'

    # Mock read job dadta errors
    with patch.object(TACCJobManager, 'get_job_data',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/read",
                {'path':'test.txt', 'data_type':'text'})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'get_job_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/read",
                {'path':'test.txt', 'data_type':'text'})
        assert response.status == '500 Internal Server Error'

    # Mock download job data errors
    with patch.object(TACCJobManager, 'download_job_data',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/download",
                {'path':'test.txt'})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'download_job_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/download",
                {'path':'test.txt'})
        assert response.status == '500 Internal Server Error'

    # Mock upload job data errors
    with patch.object(TACCJobManager, 'upload_job_data',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/upload",
                {'path':'test.txt'})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'upload_job_data',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/upload",
                {'path':'test.txt'})
        assert response.status == '500 Internal Server Error'

    # Mock peak job file errors
    with patch.object(TACCJobManager, 'peak_job_file',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/peak",
                {'path':'test.txt'})
        assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'peak_job_file',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/peak",
                {'path':'test.txt'})
        assert response.status == '500 Internal Server Error'

    # Mock submit_job errors
    with patch.object(TACCJobManager, 'submit_job',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/submit",{})
        assert response.status == '400 Bad Request'
    with patch.object(TACCJobManager, 'submit_job',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/submit",{})
        assert response.status == '500 Internal Server Error'

    # Mock cancel job errors
    with patch.object(TACCJobManager, 'cancel_job',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/cancel",{})
        assert response.status == '400 Bad Request'
    with patch.object(TACCJobManager, 'cancel_job',
            side_effect=Exception('Mock unexpected error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/cancel",{})
        assert response.status == '500 Internal Server Error'

    # Cleanup job
    response = hug.test.delete(taccjm_server,
            f"{test_jm}/jobs/{job_id}/remove",{})
    assert response.status == '200 OK'
    response = hug.test.get(taccjm_server, f"{test_jm}/jobs/list",{})
    assert response.status == '200 OK'
    assert job_id not in response.data


def test_scripts(mfa):
    """Test script operations"""

    _init(mfa)

    # Send script to taccjm
    with open('tests/test_script.sh', 'w') as f:
        f.write('echo foo\n')
    response = hug.test.post(taccjm_server,
            f"{test_jm}/scripts/deploy",
            {'script_name': 'tests/test_script.sh'})
    assert response.status == '200 OK'

    # Check script exists
    response = hug.test.get(taccjm_server,
            f"{test_jm}/scripts/list",{})
    assert response.status == '200 OK'
    assert 'test_script' in response.data

    # Run script
    response = hug.test.put(taccjm_server,
            f"{test_jm}/scripts/run",
            {'script_name': 'test_script'})
    assert response.status == '200 OK'
    assert response.data == 'foo\n'

    # Mock deploy script errors
    with patch.object(TACCJobManager, 'deploy_script',
            side_effect=Exception('Mock any error.')):
        response = hug.test.post(taccjm_server,
                f"{test_jm}/scripts/deploy",
                {'script_name': 'tests/test_script.sh'})
        assert response.status == '500 Internal Server Error'

    # Mock list script errors
    with patch.object(TACCJobManager, 'list_scripts',
            side_effect=Exception('Mock any error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/scripts/list",{})
        assert response.status == '500 Internal Server Error'

    # Mock run script errors
    with patch.object(TACCJobManager, 'run_script',
            side_effect=Exception('Mock any error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/scripts/run",
                {'script_name': 'test_script'})
        assert response.status == '500 Internal Server Error'

