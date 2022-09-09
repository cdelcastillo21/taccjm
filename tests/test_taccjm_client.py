"""
Tests for taccjm


"""
import os
import pdb
import stat
import psutil
import pytest
import posixpath
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm import taccjm_client as tc
from taccjm.exceptions import TACCJMError
from taccjm.utils import *
from taccjm.constants import *

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

# TEST JM  to use throughout tests
TEST_JM = None


def _init_client():
    """
    Initializes TEST_JM on server. Note if server not found, not server will
    be started if not found.

    """

    global TEST_JM
    if TEST_JM is None:
        # Set port to test port
        tc.set_host(port=TEST_TACCJM_PORT)

        # Kill test server
        tc.find_tjm_processes(kill=True)

        # Get input to initialize
        mfa = input('\nTACC Token:')
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


def test_list_jms():
    """ Tests operation of listing job managers available """

    # Should be just one JM initialized at the beginning of tests
    jms = tc.list_jms()
    assert jms[0]['jm_id'] == TEST_JM['jm_id']

    # Error in api call
    with patch('taccjm.taccjm.api_call') as mock_api_call:
        mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
        with pytest.raises(TACCJMError):
            jms = tc.list_jms()
        mock_api_call.reset()


def test_init_jm():
    """
    Tests operation of listing job managers available
    """

    # Error - Initialize already intialized JM
    with pytest.raises(ValueError):
        jm = tc.init_jm(TEST_JM['jm_id'], 'foo', 'bar', 'test', 123456)

    # API Error - Authentication for example
    with pytest.raises(TACCJMError):
        jm = tc.init_jm('bad_jm', TEST_JM['sys'],
                        TEST_JM['user'], 'badpw', 123456)


def test_get_jm():
    """ Tests of getting info on a jm instance. """

    # Get test JM
    jm = tc.get_jm(TEST_JM['jm_id'])
    assert jm == TEST_JM

    # Error - JM that does not exist
    with pytest.raises(TACCJMError):
        jm = tc.get_jm('does-not-exist')


def test_get_queue():
    """ Tests of getting job queue for system. """

    # Successful return
    queue = tc.get_queue(TEST_JM['jm_id'], user='all')

    # Error
    with patch('taccjm.taccjm.api_call') as mock_api_call:
        mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
        with pytest.raises(TACCJMError):
            queue = tc.get_queue(TEST_JM['jm_id'])


def test_get_allocations():
    """ Tests of allocations on jm instance """

    # Successful return
    alloc = tc.get_allocations(TEST_JM['jm_id'])

    # Error
    with patch('taccjm.taccjm.api_call') as mock_api_call:
        mock_api_call.side_effect = TACCJMError('Mock TACCJMError')
        with pytest.raises(TACCJMError):
            alloc = tc.get_allocations(TEST_JM['jm_id'])


def test_list_files():
    """ Tests get files operation """

    # Successful return - Get files in parent dir of jobs/apps/trash dir
    dirs = ['jobs', 'apps', 'trash']
    root_jm_dir = posixpath.dirname(TEST_JM['apps_dir'])
    files = tc.list_files(TEST_JM['jm_id'], path=root_jm_dir,
            attrs=['filename','st_mode'])
    assert [d in [f['filename'] for f in files] for d in dirs]
    assert all([stat.S_ISDIR(f['st_mode']) for f in files])

    # Error
    with pytest.raises(TACCJMError):
        files = tc.list_files(TEST_JM['jm_id'], path='bad/path')


def test_peak_file():
    """
    Tests peak file operation
    """

    # Empty trash dir
    tc.empty_trash(TEST_JM['jm_id'])

    # Write test file to trash dir to peak att
    text = "Hello World\n"
    test_file_path = posixpath.join(TEST_JM['trash_dir'], 'test_file.txt')
    tc.write(TEST_JM['jm_id'], text, test_file_path)
    peak = tc.peak_file(TEST_JM['jm_id'], test_file_path)
    assert text==peak

    # Error
    with pytest.raises(TACCJMError):
        peak = tc.peak_file(TEST_JM['jm_id'], '/bad/path')


def test_upload_download():
    """ Tests upload and download operations """

    # Empty trash dir
    tc.empty_trash(TEST_JM['jm_id'])

    # Write test file locally and upload to trash directory
    fname = 'test_file.txt'
    local_file = f".{fname}"
    remote_file = posixpath.join(TEST_JM['trash_dir'], fname)
    os.system(f"echo hello there > {local_file}")
    tc.upload(TEST_JM['jm_id'], local_file, remote_file)
    os.remove(local_file)

    # download file now just uploaded
    downloaded_file = '.test_download.txt'
    tc.download(TEST_JM['jm_id'], remote_file, downloaded_file)
    with open(downloaded_file, 'r') as f:
        assert f.read()=='hello there\n'
    os.remove(downloaded_file)

    # Error - Upload invalid path
    with pytest.raises(TACCJMError):
        tc.upload(TEST_JM['jm_id'], 'foo', remote_file)

    # Error - download non-existant file
    with pytest.raises(TACCJMError):
        tc.download(TEST_JM['jm_id'], 'does-not-exist', downloaded_file)


def test_remove_restore():
    """ Tests remove and restore operations """

    # Empty trash dir
    tc.empty_trash(TEST_JM['jm_id'])

    # Write test file to use and send it to the trash directory
    fname = 'test_file.txt'
    local_file = f".{fname}"
    remote_file = posixpath.join(TEST_JM['trash_dir'], fname)
    os.system(f"echo hello there > {local_file}")
    tc.upload(TEST_JM['jm_id'], local_file, remote_file)
    os.remove(local_file)

    # Remove the test file - note this will just rename it in trash dir
    tc.remove(TEST_JM['jm_id'], remote_file)
    files = tc.list_files(TEST_JM['jm_id'], TEST_JM['trash_dir'])
    trash_name = remote_file.replace('/','___')
    assert fname not in [f['filename'] for f in files]
    assert trash_name in [f['filename'] for f in files]

    # Now restore it - once again this will just rename the file
    tc.restore(TEST_JM['jm_id'], remote_file)
    files = tc.list_files(TEST_JM['jm_id'], TEST_JM['trash_dir'])
    assert fname in [f['filename'] for f in files]
    assert trash_name not in [f['filename'] for f in files]

    # Error - Remove file that does not exist
    with pytest.raises(TACCJMError):
        tc.remove(TEST_JM['jm_id'], '/bad/path')

    # Error - Restore file that does not exist
    with pytest.raises(TACCJMError):
        tc.restore(TEST_JM['jm_id'], '/bad/path')


def test_read_write():
    """ Tests write operation """

    # Empty trash dir
    tc.empty_trash(TEST_JM['jm_id'])

    # Write test file
    text = 'hello there'
    fname = 'test_write.txt'
    remote_file = posixpath.join(TEST_JM['trash_dir'], fname)
    tc.write(TEST_JM['jm_id'], text, remote_file)
    files = tc.list_files(TEST_JM['jm_id'], path=TEST_JM['trash_dir'])
    assert fname in [f['filename'] for f in files]

    # Read back in test file and make sure it matches
    read_txt = tc.read(TEST_JM['jm_id'], remote_file)
    assert read_txt==text

    # Error - write to non-existant file
    with pytest.raises(TACCJMError):
        tc.write(TEST_JM['jm_id'], text, '/bad/path')

    # Error - Read from non-existant file
    with pytest.raises(TACCJMError):
        _ = tc.read(TEST_JM['jm_id'], '/bad/path')


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

    # Remove app on remote system if any exist
    deploy_path = posixpath.join(TEST_JM['apps_dir'], app_config['name'])
    try:
        tc.remove(TEST_JM['jm_id'], deploy_path)
    except TACCJMError as t:
        if str(t).startswith("NotFound"):
            pass
        else:
            raise t

    # Deploy app
    deployed_app = tc.deploy_app(TEST_JM['jm_id'],
            app_config=app_config, local_app_dir=local_app_dir)
    assert deployed_app['name']==app_config['name']


    # Assert App just deployed exists in list of apps
    apps = tc.list_apps(TEST_JM['jm_id'])
    assert deployed_app['name'] in apps

    # Get App Config back
    deployed_config = tc.get_app(TEST_JM['jm_id'], deployed_app['name'])
    assert deployed_config['name']==app_config['name']

    # Deploy app - Local app does not exist
    with pytest.raises(FileNotFoundError):
        _ = tc.deploy_app(TEST_JM['jm_id'],
                app_config, local_app_dir='does-not-exst',
                overwrite=True)

    # Error - get non-existant app
    with pytest.raises(TACCJMError):
        _ = tc.get_app(TEST_JM['jm_id'], 'does-not-exst')

    # Cleanup - remove app locally
    os.system(f"rm -rf {test_app}")

    # Cleanup trash
    tc.empty_trash(TEST_JM['jm_id'])


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

    # Deploy app
    deployed_app = tc.deploy_app(TEST_JM['jm_id'],
            app_config=app_config, local_app_dir=local_app_dir,
            overwrite=True)

    # Set-up test job using app - Place job input file in local app dir
    job_config['allocation']=ALLOCATION
    job_config['inputs']['input1'] = os.path.join(local_app_dir, 'input.txt')
    os.system(f"echo hello world > {job_config['inputs']['input1']}")
    deployed_job = tc.deploy_job(TEST_JM['jm_id'], job_config=job_config,
            local_job_dir=local_app_dir, stage=True, desc='Test Job')
    assert 'job_id' in deployed_job.keys() and 'job_dir' in deployed_job.keys()
    assert deployed_job['desc']=='Test Job'

    # Check for job in list of jobs
    jobs = tc.list_jobs(TEST_JM['jm_id'])
    assert deployed_job['job_id'] in jobs

    # Write some data to a job file
    text = 'hello world\n'
    tc.write_job_file(TEST_JM['jm_id'],
            deployed_job['job_id'], text, 'hello.txt')

    # Read data from file just written
    data = tc.read_job_file(TEST_JM['jm_id'], deployed_job['job_id'],
            'hello.txt', data_type='text')
    assert data==text

    # Download file just written
    downloaded_file = os.path.join(local_app_dir, 'downloaded.txt')
    downloaded_file = tc.download_job_file(TEST_JM['jm_id'],
            deployed_job['job_id'],
            'hello.txt', dest_dir=local_app_dir)
    with open(downloaded_file, 'r') as f:
        assert f.read()==text

    # Rename it and Upload it back to job directory
    upload_file = os.path.join(local_app_dir,
            deployed_job['job_id'],'goodbye.txt')
    with open(upload_file, 'w') as f:
        f.write('goodbye world\n')
    tc.upload_job_file(TEST_JM['jm_id'], deployed_job['job_id'], upload_file)

    # Check file uploaded is in job directory
    files = tc.list_job_files(TEST_JM['jm_id'], deployed_job['job_id'])
    assert 'goodbye.txt' in [f['filename'] for f in files]

    # Peak at file just uploaded
    peak = tc.peak_job_file(TEST_JM['jm_id'], deployed_job['job_id'],
            path='goodbye.txt')
    assert peak=='goodbye world\n'

    # Submit job
    job = tc.submit_job(TEST_JM['jm_id'], deployed_job['job_id'])
    assert 'slurm_id' in job.keys()

    # Cancel job
    slurm_id = job['slurm_id']
    job = tc.cancel_job(TEST_JM['jm_id'], deployed_job['job_id'])
    assert 'slurm_id' not in job.keys() and 'slurm_hist' in job.keys()
    assert slurm_id in job['slurm_hist']

    # Remove job
    removed = tc.remove_job(TEST_JM['jm_id'], deployed_job['job_id'])
    assert deployed_job['job_id'] not in tc.list_jobs(TEST_JM['jm_id'])
    assert removed==deployed_job['job_id']

    # Restore job
    tc.restore_job(TEST_JM['jm_id'], removed)
    assert deployed_job['job_id'] in tc.list_jobs(TEST_JM['jm_id'])

    # Error - Get invalid job
    with pytest.raises(TACCJMError):
        _ = tc.get_job(TEST_JM['jm_id'], 'bad_job')

    # Error - Deploy job with bad path specified
    with pytest.raises(TACCJMError):
        _ = tc.deploy_job(TEST_JM['jm_id'], job_config=None,
                local_job_dir='/bad/path', stage=False, desc='Test Job')

    # Error - Write job file to bad dest_dir
    with pytest.raises(TACCJMError):
        tc.write_job_file(TEST_JM['jm_id'],
                deployed_job['job_id'], text, 'bad/path/hello.txt')

    # Error - Read job file error - file does not exist
    with pytest.raises(TACCJMError):
        _ = tc.read_job_file(TEST_JM['jm_id'], deployed_job['job_id'],
                'bad/path/hello.txt', data_type='text')

    # Error - Download job file that does not exist
    with pytest.raises(TACCJMError):
        _ = tc.download_job_file(TEST_JM['jm_id'],
                deployed_job['job_id'],
                'does/not/exist/hello.txt', dest_dir=local_app_dir)

    # Error - Upload job file that does not exist
    with pytest.raises(TACCJMError):
        tc.upload_job_file(TEST_JM['jm_id'], deployed_job['job_id'],
                '/bad/path/test.txt')

    # Error - Peak at job file that does not exist
    with pytest.raises(TACCJMError):
        peak = tc.peak_job_file(TEST_JM['jm_id'], deployed_job['job_id'],
                path='does/not/exist/goodbye.txt')

    # Error - Submit job that does not exist
    with pytest.raises(TACCJMError):
        _ = tc.submit_job(TEST_JM['jm_id'], 'bad-id')

    # Error - Cancel invalid job
    with pytest.raises(TACCJMError):
        _ = tc.cancel_job(TEST_JM['jm_id'], 'bad-id')

    # Cleanup local app dir
    os.system(f"rm -rf {local_app_dir}")


def test_scripts():
    """Test script operations"""

    py_script = os.path.join(os.getcwd(), 'test-py.py')
    with open(py_script, 'w') as f:
        f.write('import os\nprint("hello world")\n')

    # Send script to taccjm
    tc.deploy_script(TEST_JM['jm_id'], py_script)

    # Check script exists
    scripts = tc.list_scripts(TEST_JM['jm_id'])
    assert 'test-py' in scripts

    # Run script
    result = tc.run_script(TEST_JM['jm_id'], 'test-py')
    assert result  == 'hello world\n'

    # Cleanup local app dir
    os.system(f"rm -rf {py_script}")


_init_client()
