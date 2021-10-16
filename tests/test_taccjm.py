"""
Tests for TACC JobManager Class


"""
import os
import pdb
import pytest
import posixpath
from dotenv import load_dotenv
from unittest.mock import patch

from taccjm.TACCJobManager import TACCJobManager, TJMCommandError
from taccjm.SSHClient2FA import SSHClient2FA
from paramiko import SSHException

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Note: .env file in tests directory must contain bellow params to run tests:
load_dotenv()

global SYSTEM, USER, PW, SYSTEM, ALLOCATION
USER = os.environ.get("TACCJM_USER")
PW = os.environ.get("TACCJM_PW")
SYSTEM = os.environ.get("TACCJM_SYSTEM")
ALLOCATION = os.environ.get("TACCJM_ALLOCATION")

# JM will be the job manager instance that should be initialized once but used
# by all tests. Note the test_init test initializes the JM to begin with,
# but if only running one other test, the first test to run will initialize
# the JM for the test session.
mfa = input('\nTACC Token:')
JM = None
JM = TACCJobManager(SYSTEM, user=USER,
        psw=PW, mfa=mfa, working_dir="test-taccjm")


def _setup_local_test_files():
    """Setup test directory and file"""
    # Create a test file and folder
    cwd = os.getcwd()
    test_fname = "test.txt"
    test_folder = f"{cwd}/.test"
    test_file = f"{test_folder}/{test_fname}"
    _ = os.system(f"rm -rf {test_folder}")
    _ = os.system(f"mkdir {test_folder}")
    _ = os.system(f"echo hello world > {test_file}")
    _ = os.system(f"echo hello again >> {test_file}")

    return test_fname, test_file, test_folder

def _cleanup_local_test_files():
    """Remove test directory and file"""
    cwd = os.getcwd()
    test_folder = f"{cwd}/.test"
    _ = os.system(f"rm -rf {test_folder}")


def test_init():
    """Testing initializing class and class helper functions"""

    # Invalid TACC system specified
    with pytest.raises(ValueError):
        bad = TACCJobManager("foo", user=USER, psw=PW, mfa=123456)

    # Invalid working directory specified, no tricky business allowed with ..
    with pytest.raises(ValueError):
        bad = TACCJobManager(SYSTEM, user=USER,
                psw=PW, mfa=123456, working_dir="../test-taccjm")
    with pytest.raises(ValueError):
        bad = TACCJobManager(SYSTEM, user=USER,
                psw=PW, mfa=123456, working_dir="test-taccjm/..")
    with pytest.raises(ValueError):
        bad = TACCJobManager(SYSTEM, user=USER,
                psw=PW, mfa=123456, working_dir="test-taccjm/../test")

    # Command that should work, also test printing to stdout the output
    assert JM._execute_command('echo test') == 'test\n'

     # Tests command that fails due to SSH error, which we mock
    with patch.object(SSHClient2FA, 'exec_command',
            side_effect=SSHException('Mock ssh exception')):
        with pytest.raises(SSHException):
             JM._execute_command('echo test')

    # Test commands that fails because of non-zero return code
    with pytest.raises(TJMCommandError):
        JM._execute_command('foo')

    # Test making directory (remove it first)
    test_dir = posixpath.join(JM.trash_dir, 'test')
    try:
        JM._execute_command(f"rmdir {test_dir}")
    except:
        pass
    JM._mkdir(test_dir)

    # Test making directory that will fail
    with pytest.raises(TJMCommandError):
        JM._mkdir(posixpath.join(JM.trash_dir, 'test/will/fail'))


def test_showq():
    """Test accessing TACC queue"""

    # Get queue for all users
    queue = JM.showq(user='all')
    assert len(queue)>0
    queue_fields = ['job_id', 'job_name', 'username',
            'state', 'nodes', 'remaining', 'start_time']
    assert all([q in queue[0].keys() for q in queue_fields])

    # Fail to get queue
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'showq', 1,
                            'mock error', '', 'mock error')):
        with pytest.raises(TJMCommandError) as t:
            bad_queue = JM.showq()


def test_get_allocations():
    """Test accessing TACC allocations"""

    # Get allocations
    allocations = JM.get_allocations()
    assert len(allocations)>0
    allocation_fields = ['name', 'service_units', 'exp_date']
    assert all([a in allocations[0].keys() for a in allocation_fields])

    # Fail to get allocations
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER,
                '/usr/local/etc/taccinfo', 1, 'mock error', '', 'mock error')):
        with pytest.raises(TJMCommandError) as t:
            bad_queue = JM.get_allocations()


def test_list_files():
    """Test getting info on file and folders in remote directories"""

    # Create a test file and folder and empty trash directory
    test_fname, test_file, test_folder, = _setup_local_test_files()
    JM.empty_trash()

    # Upload directory with file in it
    dest_name = 'test_dir'
    dest_dir = '/'.join([JM.trash_dir, dest_name])
    JM.upload(test_folder, dest_dir)

    # Now get folder info of folder uploaded
    files = JM.list_files(dest_dir)
    assert len(files)==1
    assert test_fname==files[0]['filename']
    assert 24==files[0]['st_size']

    # Now get info on file only in folder
    remote_file_path = '/'.join([dest_dir, test_fname])
    file = JM.list_files(remote_file_path)
    assert len(file)==1
    assert remote_file_path==file[0]['filename']
    assert 24==file[0]['st_size']

    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=FileNotFoundError('Mock file not found error')):
        with pytest.raises(FileNotFoundError):
            JM.list_files(dest_dir)
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=PermissionError('Mock file permission error')):
        with pytest.raises(PermissionError):
            JM.list_files(dest_dir)
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=Exception('Mock unexpected error')):
        with pytest.raises(Exception):
            JM.list_files(dest_dir)

    # Cleanup local and remote files
    JM.empty_trash()
    _cleanup_local_test_files()


def test_peak_file():
    """Test peak_file operations"""

    # Create a test file and folder
    test_fname, test_file, test_folder = _setup_local_test_files()

    # Upload directory with file in it
    dest_fname = 'test_path'
    dest_path = '/'.join([JM.trash_dir, dest_fname])
    JM.upload(test_file, dest_path)

    # Now peak at first line in file
    first_line = JM.peak_file(dest_path, head=1)
    assert first_line=='hello world\n'

    # Now peak at last line in file
    last_line = JM.peak_file(dest_path, tail=1)
    assert last_line=='hello again\n'

    # Now peak again at first line
    both_lines = JM.peak_file(dest_path)
    assert both_lines=='hello world\nhello again\n'

    # Mock permission, file not found, and unexpected peak file errors
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'head', 1,
                             'Permission denied',
                             '', 'Mock permission error')):
        with pytest.raises(PermissionError) as p:
            JM.peak_file(test_file)
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'head', 1,
                             'Not a directory', '', 'Mock file not found')):
        with pytest.raises(FileNotFoundError) as f:
            JM.peak_file(test_file)
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'head', 1,
                             'Unexpected error', '', 'Mock unexpected error')):
        with pytest.raises(TJMCommandError) as t:
            JM.peak_file(test_file)

    # Cleanup local and remote files
    JM.remove(dest_path)
    _cleanup_local_test_files()


def test_upload():
    """Test uploadng a file and folder"""

    # Create a test file and folder
    test_fname, test_file, test_folder = _setup_local_test_files()

    # Send file - Try sending file only to trash directory
    dest_name = 'test_file'
    dest_path = '/'.join([JM.trash_dir, dest_name])
    JM.upload(test_file, dest_path)
    files = JM.list_files(JM.trash_dir)
    assert dest_name in [f['filename'] for f in files]

    # Send directory - Try sending directory now
    dest_name = 'test_dir'
    dest_dir = '/'.join([JM.trash_dir, dest_name])
    JM.upload(test_folder, dest_dir)
    files = JM.list_files(JM.trash_dir)
    assert dest_name in [f['filename'] for f in files]
    files = JM.list_files(dest_dir)
    assert test_fname in [f['filename'] for f in files]

    # Try sending a file that doesn't exist
    with pytest.raises(FileNotFoundError):
        JM.upload('./does-not-exist', dest_path)

    # Now mock permission and untar-ing error, and unexpcted error
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=PermissionError('Mock file permission')):
        with pytest.raises(PermissionError):
            JM.upload(test_folder, dest_dir)
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=Exception('Mock other error')):
        with pytest.raises(Exception):
            JM.upload(test_file, dest_path)
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'tar...', 1,
                            'mock tar error', '', 'mock tar error')):
        with pytest.raises(TJMCommandError) as t:
            JM.upload(test_folder, dest_dir)

    # Remove test folder and file we sent and local test folder
    JM.remove(dest_dir)
    JM.remove(dest_path)
    _cleanup_local_test_files()

def test_download():
    """Test downloading files/folders"""

    # Create a test file and folder
    test_fname, test_file, test_folder = _setup_local_test_files()

    # Upload directory with file in it
    dest_dirname = 'test_path'
    dest_path = '/'.join([JM.trash_dir, dest_dirname])
    JM.upload(test_folder, dest_path)

    # Now download file inside folder just uploaded
    remote_fpath = '/'.join([dest_path, test_fname])
    download_path = os.path.join(test_folder, 'test_downloaded.txt')
    JM.download(remote_fpath, download_path)
    with open(download_path, 'r') as f1, open(test_file, 'r') as f2:
        assert f1.read()==f2.read()

    # Now download full folder just uploaded
    JM.download(dest_path, test_folder)
    assert dest_dirname in os.listdir(test_folder)
    downloaded_file = os.path.join(test_folder, dest_dirname, test_fname)
    with open(downloaded_file, 'r') as f1, open(test_file, 'r') as f2:
        assert f1.read()==f2.read()

    # This should mock a false tar warning, but then trigger file not found
    # becaseu tar does not exist on remote system.
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'tar...', 1,
                            '', 'padding with zeros', 'mock tar error')):
        with pytest.raises(FileNotFoundError) as t:
            JM.download(dest_path, test_folder)

    # Mock other critical tar error
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'tar...', 1,
                            'critical error', '', 'mock tar error')):
        with pytest.raises(TJMCommandError) as t:
            JM.download(dest_path, test_folder)

    # Mock permission error
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=PermissionError('Mock file permission')):
        with pytest.raises(PermissionError):
            JM.download(dest_path, test_folder)

    # Remove test folder and file we sent and local test folder
    JM.remove(dest_path)
    _cleanup_local_test_files()


def test_files():
    """Test listing, sending, and getting files and directories"""

    # List files in path that exists and doesnt exist
    with pytest.raises(FileNotFoundError):
         JM.list_files('/bad/path')

    # Send file - Try sending test application script to apps directory
    test_file = '/'.join([JM.apps_dir, 'test_file'])
    JM.upload('./tests/test_app/assets/run.sh', test_file)
    files = JM.list_files(JM.apps_dir)
    assert 'test_file' in [f['filename'] for f in files]

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
    test_folder = JM.apps_dir + '/test_folder'
    JM.upload('./tests/test_app/assets', test_folder)
    files = JM.list_files(JM.apps_dir)
    assert 'test_folder' in [f['filename'] for f in files]

    # Get test file
    JM.download(test_file, './tests/test_file')
    assert os.path.isfile('./tests/test_file')
    os.remove('./tests/test_file')

    # Get test folder
    JM.download(test_folder, './tests/test_folder')
    assert os.path.isdir('./tests/test_folder')
    assert os.path.isfile('./tests/test_folder/run.sh')
    os.system('rm -rf ./tests/test_folder')

    # Remove file - Remove file initially sent (test application script in apps directory)
    test_file = '/'.join([JM.apps_dir, 'test_file'])
    trash_file = test_file.replace('/','___')
    JM.remove(test_file)
    files = JM.list_files(JM.apps_dir)
    trash_files = JM.list_files(JM.trash_dir)
    assert 'test_file' not in [f['filename'] for f in files]
    assert trash_file in [f['filename'] for f in trash_files]

    # Restore file removed
    test_file = '/'.join([JM.apps_dir, 'test_file'])
    JM.restore(test_file)
    files = JM.list_files(JM.apps_dir)
    assert 'test_file' in [f['filename'] for f in files]

    # Now try to remove files that don't exist and don't have permission to. Should throw errors
    with pytest.raises(Exception):
         JM.remove('/does/not/exist')
    with pytest.raises(Exception):
         JM.remove('/tmp')


def test_data():
    """Test sending and receiving data."""
    global JM

    _check_init()

    # Send text and json data
    JM.send_data('foo','test_text_data')
    JM.send_data({'foo':'bar'},'test_json_data')
    files = [f['filename'] for f in JM.list_files()]
    assert 'test_text_data' in files and 'test_json_data' in files

    # Read text and json data
    text_data = JM.get_data('test_text_data', 'text')
    json_data = JM.get_data('test_json_data', 'json')
    assert text_data=='foo'
    assert json_data=={'foo':'bar'}

    # Send bad data type, and data to non-existant path, or path where we have no permission
    with pytest.raises(ValueError):
        JM.send_data(['foo'],'foo')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=FileNotFoundError('Mock file not found')):
        with pytest.raises(FileNotFoundError):
            JM.send_data('foo', '/does/not/exist')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=PermissionError('Mock file permission')):
        with pytest.raises(PermissionError):
            JM.send_data('foo', '/canthere')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=Exception('Mock other error')):
        with pytest.raises(Exception):
            JM.send_data('foo', '/other')

    # Get data from non-existant path, or path where we have no permission
    with pytest.raises(ValueError):
        JM.send_data('foo', data_type='bad-type')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=FileNotFoundError('Mock file not found')):
        with pytest.raises(FileNotFoundError):
            JM.get_data('/does/not/exist')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=PermissionError('Mock file permission')):
        with pytest.raises(PermissionError):
            JM.get_data('/canthere')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=Exception('Mock other error')):
        with pytest.raises(Exception):
            JM.get_data('/other')


def test_templating():
    """Test loading project configuration files and templating json files"""
    global JM
    _check_init()

    proj_conf_path = './tests/test_app/project.ini'
    app_config = JM.load_templated_json_file('./tests/test_app/app.json', proj_conf_path)
    assert app_config['name']=='test_app--1.0.0'
    with pytest.raises(FileNotFoundError):
        JM.load_templated_json_file('./tests/test_app/not_found.json', proj_conf_path)

def test_deploy_app():
    """Test deploy applications """
    global JM
    _check_init()

    # Test deploying app when sending files to system is failing
    with patch.object(TACCJobManager, 'upload', side_effect=Exception('Mock file send error')):
        with pytest.raises(Exception) as e:
            bad_deploy = JM.deploy_app(local_app_dir='./tests/test_app', overwrite=True)
    # Test deploying app with bad config (missing required config)
    with pytest.raises(Exception) as e:
        bad_deploy = JM.deploy_app(local_app_dir='./tests/test_app', app_config_file='app_2.json',
                overwrite=True)

    # Deploy app (should not exist to begin with)
    test_app = JM.deploy_app(local_app_dir='./tests/test_app', overwrite=True)
    assert test_app['name']=='test_app--1.0.0'

    # Now try without overwrite and this will fail
    with pytest.raises(Exception):
        test_app = JM.deploy_app(local_app_dir='./tests/test_app')


def test_jobs():
    """Test setting up a job."""
    global JM
    _check_init()

    # Make sure app is deployed
    test_app = JM.deploy_app(local_app_dir='./tests/test_app', overwrite=True)

    # Now try setting up test job, but don't stage inputs
    test_config = JM.setup_job(local_job_dir='./tests/test_app',
            job_config_file='job.json', stage=False)
    assert test_config['appId']=='test_app--1.0.0'
    # We didn't stage the inputs, so this should hold
    assert test_config['job_id'] not in JM.get_jobs()

    # Now try setting up test job
    job_config = JM.setup_job(local_job_dir='./tests/test_app', job_config_file='job.json')
    assert job_config['appId']=='test_app--1.0.0'

    # Get job we se just set up
    jobs = JM.get_jobs()
    assert job_config['job_id'] in jobs

    # Fail setting up job -> Sending input file fails
    with patch.object(TACCJobManager, 'upload',
            side_effect=Exception('Mock upload file error')):
        with pytest.raises(Exception) as e:
            job_config = JM.setup_job(local_job_dir='./tests/test_app', job_config_file='job.json')

    # Update job config to include email and allocation
    job_config = JM.setup_job(job_config,
            email="test@test.com", allocation=ALLOCATION)
    assert job_config['email']=="test@test.com"
    assert job_config['allocation']==ALLOCATION

    # Get job config now, should be updated
    new_job_config = JM.get_job(job_config['job_id'])
    assert new_job_config['email']=="test@test.com"
    assert new_job_config['allocation']==ALLOCATION

    # Fail to save job config -> Example bad job_dir path
    with pytest.raises(Exception) as e:
        bad_config = job_config
        bad_config['job_dir'] = '/bad/path'
        JM._save_job_config(bad_config)

    # Fail to load job config -> Example: bad job id
    with pytest.raises(Exception) as e:
        bad_job = JM.get_job('bad_job')

    # Get input job file from job with input file
    input_file_path = JM.download_job_data(job_config['job_id'], 'test_input_file',
            dest_dir='./tests')
    with open(input_file_path, 'r') as f:
        assert f.read()=="hello\nworld\n"
    os.remove('./tests/test_input_file')

    # Get job file that doesn't exist
    with pytest.raises(Exception) as e:
        bad_file = JM.download_job_data(job_config['job_id'], 'bad_file')

    # Fail to get job file (some download error maybe)
    with patch.object(TACCJobManager, 'download',
            side_effect=Exception('Mock download file error')):
        with pytest.raises(Exception) as e:
            bad_file = JM.download_job_data(job_config['job_id'], 'test_input_file', dest_dir='./tests')

    # Cleanup files just downloaded
    os.remove(input_file_path)
    os.rmdir(os.path.join('.', 'tests', job_config['job_id']))

    # Send file - Try sending this script to job directory
    sent_file_path = JM.upload_job_data(job_config['job_id'],
        './tests/test_taccjm.py', dest_dir='.')
    job_files = JM.ls_job(job_config['job_id'])
    assert 'test_taccjm.py' in job_files

    # Fail to send job file
    with pytest.raises(Exception) as e:
        bad_send = JM.upload_job_data(job_config['job_id'], './tests/bad_file', dest_dir='.')

    # Peak at job we just sent (this script, should start with a comment line)
    input_file_text = JM.peak_job_file(job_config['job_id'], 'test_taccjm.py')
    assert input_file_text.startswith('"""')


    # Fail to submit job because SLURM error
    with patch.object(TACCJobManager, '_execute_command',
            return_value="FAILED\n"):
        with pytest.raises(Exception) as e:
            bad_submit = JM.submit_job(job_config['job_id'])

    # Cancel job before its submitted
    with pytest.raises(Exception) as e:
        bad_cancel = JM.cancel_job(job_config['job_id'])

    # Succesfully submit job
    submitted_job = JM.submit_job(job_config['job_id'])
    assert submitted_job['slurm_id'] is not None

    # Fail to try to submit job again
    with pytest.raises(Exception) as e:
        _ = JM.submit_job(job_config['job_id'])

    # Forced failure to cancel job -> slurm error
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=Exception('Mock slurm error')):
        with pytest.raises(Exception) as e:
            _ = JM.cancel_job(job_config['job_id'])

    # Successfully cancel job we just submitted
    canceled = JM.cancel_job(job_config['job_id'])

    # Fail to re-cancel job
    with pytest.raises(Exception) as e:
        bad_cancel = JM.cancel_job(job_config['job_id'])

    # Fail to submit job because of slurm error
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=Exception('Execute command error')):
        with pytest.raises(Exception) as e:
            bad_deploy = JM.cancel_job(job_config['job_id'])

    # Cleanup non existent job
    bad_cleanup = JM.cleanup_job('bad_job')

    # Cleanup jobs we set-up
    _ = JM.cleanup_job(job_config['job_id'])



# def test_main(capsys):
#     """CLI Tests"""
#     # capsys is a pytest fixture that allows asserts agains stdout/stderr
#     # https://docs.pytest.org/en/stable/capture.html
#     main(["7"])
#     captured = capsys.readouterr()
#     assert "The 7-th Fibonacci number is 13" in captured.out
