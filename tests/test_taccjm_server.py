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


def _init_server():
    """Call once to initailize JM for tests"""
    global initialized
    if not initialized:
        mfa = input("\nTACC Token:")
        init_args = {'jm_id': test_jm, 'system': SYSTEM,
                     'user': USER, 'psw': PW, 'mfa':mfa}
        response = hug.test.post(taccjm_server, 'init', init_args)
        initialized = True


def test_jms():
    """
    Test managing JM instances and errors
    """

    # List JMs - There should be none
    response = hug.test.get(taccjm_server, 'list', None)
    assert response.status == '200 OK'

    # Get JM before added - error
    bad_jm = 'not_init'
    error = {'jm_error': f"TACCJM {bad_jm} does not exist."}
    response = hug.test.get(taccjm_server, f"{bad_jm}", None)
    assert response.status == '404 Not Found'
    assert response.data['errors'] == error

    # Try initializing JM with same jm_id -> Should give error
    init_args = {'jm_id': test_jm, 'system': SYSTEM,
                 'user': USER, 'psw': PW, 'mfa':123456}
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
                     'user': 'foo', 'psw': PW, 'mfa':123456}
    response = hug.test.post(taccjm_server, 'init', bad_init_args)
    assert response.status == '401 Unauthorized'

    # Initialize JM again, but with bad system, should get Not Found error
    bad_init_args = {'jm_id': 'bad_jm', 'system': 'foo',
                     'user': USER, 'psw': PW, 'mfa':123456}
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
    local_path = os.path.join(os.getcwd(), 'test_download')
    response = hug.test.get(taccjm_server, f"{test_jm}/files/download",
            {'remote_path':'test_file', 'local_path':local_path})
    assert response.status == '200 OK'
    assert 'test_download' in os.listdir(os.getcwd())
    os.remove(local_path)

    # Empty trash
    response = hug.test.delete(taccjm_server, f"{test_jm}/trash/empty", {})
    assert response.status == '200 OK'


def test_read_write():
    """Test read and write file operations"""

    # Send local data to a file
    response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
            {'data':'foo', 'remote_path':'test_data'})
    assert response.status == '200 OK'
    response = hug.test.get(taccjm_server, f"{test_jm}/files/list")
    assert 'test_data' in [f['filename'] for f in response.data]

    # Send data errors
    response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
            {'data':['foo'], 'remote_path':'test_data'})
    assert response.status == '400 Bad Request'
    response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
            {'data':'foo', 'remote_path':'foo/bar/test_data'})
    assert response.status == '404 Not Found'
    with patch.object(TACCJobManager, 'write',
            side_effect=PermissionError('Mock unexpected error.')):
        response = hug.test.put(taccjm_server, f"{test_jm}/files/write",
                {'data':'foo', 'remote_path':'/test_data'})
        assert response.status == '403 Forbidden'

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
    with patch.object(TACCJobManager, 'read',
            side_effect=PermissionError('Mock permission error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/files/read",
            {'remote_path':'/test_data', 'data_type':'text'})
        assert response.status == '403 Forbidden'

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


def test_apps():
    """Test app operations"""

    # Name of test app directory locally
    test_app = '.test-app'
    dest_dir = os.getcwd()
    local_app_dir = os.path.join(dest_dir, test_app)

    # Remove app locally if exists
    os.system(f"rm -rf {test_app}")

    # Create template app locally
    app_config, job_config = create_template_app(test_app, dest_dir=dest_dir)

    response = hug.test.post(taccjm_server, f"{test_jm}/apps/deploy",
            {'local_app_dir': local_app_dir, 'overwrite':True})
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

    # Mock get errors
    with patch.object(TACCJobManager, 'get_app',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.get(taccjm_server, f"{test_jm}/apps/test_app",
            {'app_config':app_config})
        assert response.status == '400 Bad Request'

    # Cleanup - remove app locally
    os.system(f"rm -rf {test_app}")


def test_jobs():
    """Test job operations"""

    # Name of test app directory locally
    test_app = '.test-app'
    dest_dir = os.getcwd()
    local_app_dir = os.path.join(dest_dir, test_app)

    # Remove app locally if exists
    os.system(f"rm -rf {test_app}")

    # Create template app locally
    app_config, job_config = create_template_app(test_app, dest_dir=dest_dir)

    # Deploy test app
    response = hug.test.post(taccjm_server, f"{test_jm}/apps/deploy",
            {'local_app_dir': local_app_dir, 'overwrite':True})

    # Set-up test job using app - Place job input file in local app dir
    job_config['allocation']=ALLOCATION
    job_config['inputs']['input1'] = os.path.join(local_app_dir, 'input.txt')
    os.system(f"echo hello world > {job_config['inputs']['input1']}")
    response = hug.test.post(taccjm_server, f"{test_jm}/jobs/deploy",
            {'job_config':job_config, 'local_job_dir':local_app_dir})
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
            {'path':'hello.txt', 'dest_dir': local_app_dir})
    assert response.status == '200 OK'
    with open(os.path.join(local_app_dir, job_id,'hello.txt'), 'r') as f:
        assert f.read() == 'hello world\n'

    # Rename it and Upload it back to job directory
    with open(os.path.join(local_app_dir, job_id,'goodbye.txt'), 'w') as f:
        f.write('goodby world\n')
    response = hug.test.put(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/upload",
            {'path':os.path.join(local_app_dir, job_id,'goodbye.txt')})
    assert response.status == '200 OK'

    # Check file uploaded is in job directory
    response = hug.test.get(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/list",
            {})
    assert response.status == '200 OK'
    assert 'goodbye.txt' in [f['filename'] for f in response.data]

    # Peak at file just uploaded
    response = hug.test.get(taccjm_server,
            f"{test_jm}/jobs/{job_id}/files/peak",
            {'path':'goodbye.txt'})
    assert response.status == '200 OK'
    assert response.data == 'goodby world\n'

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
        assert response.status == '400 Bad Request'

    # Mock deploy_job errors
    with patch.object(TACCJobManager, 'deploy_job',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.post(taccjm_server, f"{test_jm}/jobs/deploy",
                {'job_config':job_config})
        assert response.status == '400 Bad Request'

    # Mock write job data errors
    with patch.object(TACCJobManager, 'write_job_file',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/write",
                {'path':'test.txt', 'data':'foo'})
        assert response.status == '400 Bad Request'
    with patch.object(TACCJobManager, 'write_job_file',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/write",
                {'path':'test.txt', 'data':'foo'})
        assert response.status == '404 Not Found'

    # Mock read job dadta errors
    with patch.object(TACCJobManager, 'read_job_file',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/read",
                {'path':'test.txt', 'data_type':'text'})
        assert response.status == '404 Not Found'

    # Mock download job data errors
    with patch.object(TACCJobManager, 'download_job_file',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/download",
                {'path':'test.txt'})
        assert response.status == '404 Not Found'

    # Mock upload job data errors
    with patch.object(TACCJobManager, 'upload_job_file',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/upload",
                {'path':'test.txt'})
        assert response.status == '404 Not Found'

    # Mock peak job file errors
    with patch.object(TACCJobManager, 'peak_job_file',
            side_effect=FileNotFoundError('Mock file error.')):
        response = hug.test.get(taccjm_server,
                f"{test_jm}/jobs/{job_id}/files/peak",
                {'path':'test.txt'})
        assert response.status == '404 Not Found'

    # Mock submit_job errors
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'sbatch', 1,
                            'mock sbatch error', '', 'mock sbatch error')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/submit",{})
        assert response.status == '500 Internal Server Error'

    # Mock cancel job errors
    with patch.object(TACCJobManager, 'cancel_job',
            side_effect=ValueError('Mock value error.')):
        response = hug.test.put(taccjm_server,
                f"{test_jm}/jobs/{job_id}/cancel",{})
        assert response.status == '400 Bad Request'

    # Remove job
    response = hug.test.delete(taccjm_server,
            f"{test_jm}/jobs/{job_id}/remove",{})
    assert response.status == '200 OK'
    response = hug.test.get(taccjm_server, f"{test_jm}/jobs/list",{})
    assert job_id not in response.data

    # Restore job
    response = hug.test.post(taccjm_server,
            f"{test_jm}/jobs/{job_id}/restore",{})
    assert response.status == '200 OK'
    response = hug.test.get(taccjm_server, f"{test_jm}/jobs/list",{})
    assert job_id in response.data

    # Cleanup local app dir
    os.system(f"rm -rf {local_app_dir}")


def test_scripts():
    """Test script operations"""

    py_script = os.path.join(os.getcwd(), 'test-py.py')
    with open(py_script, 'w') as f:
        f.write('import os\nprint("hello world")\n')

    # Send script to taccjm
    response = hug.test.post(taccjm_server,
            f"{test_jm}/scripts/deploy",
            {'script_name': py_script})
    assert response.status == '200 OK'

    # Check script exists
    response = hug.test.get(taccjm_server,
            f"{test_jm}/scripts/list",{})
    assert response.status == '200 OK'
    assert 'test_script' in response.data

    # Run script
    response = hug.test.put(taccjm_server,
            f"{test_jm}/scripts/run",
            {'script_name': 'test-py'})
    assert response.status == '200 OK'
    assert response.data == 'hello world\n'

    # Remove test script
    os.remove(py_script)


# Initialize JM for server tests
_init_server()

