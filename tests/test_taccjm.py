"""
Tests for TACC JobManager Class


Note:


References:

"""
import os
import pdb
import pytest

from dotenv import load_dotenv
from taccjm.TACCJobManager import TACCJobManager

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Note: .env file in tests directory must contain TACC_USER and TACC_PW variables defined
load_dotenv()
USER = os.environ.get("TACC_USER")
PW = os.environ.get("TACC_PW")

# JM will be the job manager instance that should be initialized once but used by all tests.
# Note the test_init test initializes the JM to begin with, but if only running one other test,
# the first test to run will initialize the JM for the test session.
global JM
JM = None


def _check_init(system, mfa):
    global JM
    if JM is None:
        # Initialize taccjm that will be used for tests - use special tests dir
        JM = TACCJobManager(system, user=USER, psw=PW, mfa=mfa, apps_dir="test-taccjm-apps",
                jobs_dir="test-taccjm-jobs", trash_dir="test-taccjm-trash")


def test_init(system, mfa):
    """Testing initializing systems"""

    global JM
    # Initialize taccjm that will be used for tests - use special tests dir
    JM = TACCJobManager(system, user=USER, psw=PW, mfa=mfa, apps_dir="test-taccjm-apps",
            jobs_dir="test-taccjm-jobs")

    with pytest.raises(Exception):
        bad = TACCJobManager("foo", user=USER, psw=PW, mfa=mfa)

    # Command that should work, also test printing to stdout the output 
    assert JM._execute_command('echo test', prnt=True) == 'test\n'

    # Tests command that fails
    with pytest.raises(Exception):
         JM._execute_command('foo')

    # Test show queue and get allocation
    assert f"SUMMARY OF JOBS FOR USER: <{USER}>" in JM.showq()
    assert f"Project balances for user {USER}" in JM.get_allocations()


def test_files(system, mfa):
    """Test listing, sending, and getting files and directories"""
    global JM

    _check_init(system, mfa)

    # List files in path that exists and doesnt exist
    assert 'test-taccjm-apps' in JM.list_files()
    with pytest.raises(FileNotFoundError):
         JM.list_files('/bad/path')

    # Send file - Try sending test application script to apps directory
    test_file = '/'.join([JM.apps_dir, 'test_file'])
    assert 'test_file' in JM.send_file('./tests/test_app/assets/run.sh',
            test_file)

    # Test peaking at a file just sent
    first = '#### BEGIN SCRIPT LOGIC'
    first_line = JM.peak_file(test_file, head=1)
    assert first in first_line
    last = '${command} ${command_opts} >>out.txt 2>&1'
    last_line = JM.peak_file(test_file, tail=1)
    assert last in last_line
    both_lines = JM.peak_file(test_file)

    with pytest.raises(Exception):
         JM.peak_file('/bad/path')

    # Send directory - Now try sending whole assets directory
    test_folder = '/'.join([JM.apps_dir, 'test_folder'])
    assert 'test_folder' in JM.send_file('./tests/test_app/assets',
            '/'.join([JM.apps_dir, 'test_folder']))
    assert '.hidden_file' not in JM.list_files(path=test_folder)

    # Send directory - Now try sending whole assets directory, include hidden files
    test_folder_hidden = '/'.join([JM.apps_dir, 'test_folder_hidden'])
    assert 'test_folder_hidden' in JM.send_file('./tests/test_app/assets',
            '/'.join([JM.apps_dir, 'test_folder_hidden']), exclude_hidden=False)
    assert '.hidden_file' in JM.list_files(path='/'.join([JM.apps_dir, 'test_folder_hidden']))

    # Get test file
    JM.get_file(test_file, './tests/test_file')
    assert os.path.isfile('./tests/test_file')

    # Get test folder
    JM.get_file(test_folder, './tests/test_folder')
    assert os.path.isdir('./tests/test_folder')
    assert os.path.isfile('./tests/test_folder/run.sh')


def test_templating(system, mfa):
    """Test loading project configuration files and templating json files"""
    global JM
    _check_init(system, mfa)

    proj_conf = JM.load_project_config('./tests/test_app/project.ini')
    assert proj_conf['app']['name']=='test_app'
    assert proj_conf['app']['version']=='1.0.0'
    with pytest.raises(FileNotFoundError):
        JM.load_project_config('./tests/test_app/does_not_exist.ini')

    app_config = JM.load_templated_json_file('./tests/test_app/app.json', proj_conf)
    assert app_config['name']=='test_app--1.0.0'
    with pytest.raises(FileNotFoundError):
        JM.load_templated_json_file('./tests/test_app/not_found.json', proj_conf)


def test_deploy_app(system, mfa):
    """Test deploy applications """
    global JM
    _check_init(system, mfa)

    # Deploy app (should not exist to begin with)
    test_app = JM.deploy_app(local_app_dir='./tests/test_app', overwrite=True)
    assert test_app['name']=='test_app--1.0.0'

    # Now try without overwrite and this will fail
    with pytest.raises(Exception):
        test_app = JM.deploy_app(local_app_dir='./tests/test_app')

    # Get the wrapper script
    wrapper_script = JM.get_app_wrapper_script(test_app['name'])
    assert "${command} ${command_opts} >>out.txt 2>&1" in wrapper_script

    # Load the app config, it should match
    loaded_config = JM.load_app_config(test_app['name'])
    assert loaded_config==test_app
    with pytest.raises(Exception):
        JM.load_app_config('does_not_exist')


def test_setup_job(system, mfa):
    """Test setting up a job."""
    global JM
    _check_init(system, mfa)

    # Make sure app is deployed 
    test_app = JM.deploy_app(local_app_dir='./tests/test_app', overwrite=True)

    # Now try setting up test job
    job_config = JM.setup_job(local_job_dir='./tests/test_app', job_config_file='job.json')
    assert job_config['appId']=='test_app--1.0.0'


# def test_main(capsys):
#     """CLI Tests"""
#     # capsys is a pytest fixture that allows asserts agains stdout/stderr
#     # https://docs.pytest.org/en/stable/capture.html
#     main(["7"])
#     captured = capsys.readouterr()
#     assert "The 7-th Fibonacci number is 13" in captured.out
