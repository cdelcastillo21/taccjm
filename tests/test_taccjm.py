"""
Tests for TACC JobManager Class


"""
import os
import pdb
import pytest
import posixpath
from dotenv import load_dotenv
from unittest.mock import patch
from taccjm.utils import create_template_app

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
    JM.empty_trash()
    test_fname, test_file, test_folder, = _setup_local_test_files()

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
    JM.empty_trash()
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
    JM.empty_trash(dest_path)
    _cleanup_local_test_files()


def test_upload():
    """Test uploadng a file and folder"""

    # Create a test file and folder and empty trash
    JM.empty_trash()
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
    JM.empty_trash()
    _cleanup_local_test_files()

def test_download():
    """Test downloading files/folders"""

    # Empty trash Create a test file and folder
    JM.empty_trash()
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
    # because tar does not exist on remote system.
    # Note: temporary tar file .test.tar.gz should be cleaned up here
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'tar...', 1,
                            '', 'padding with zeros', 'mock tar error')):
        with pytest.raises(FileNotFoundError) as t:
            JM.download(dest_path, test_folder)
            local_tar = f"{os.path.split(test_folder)[1]}.tar.gz."
            assert local_tar not in os.listdir('.')


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

    # Empty trash dir and clean local files to conclude tests
    JM.empty_trash()
    _cleanup_local_test_files()


def test_remove():
    """Test removing files on remote system"""

    # Empty trash dir and create a test file and folder
    JM.empty_trash()
    test_fname, test_file, test_folder = _setup_local_test_files()

    # Upload directory with file in it
    dest_dirname = 'test_path'
    dest_path = '/'.join([JM.trash_dir, dest_dirname])
    JM.upload(test_folder, dest_path)

    # Now remove file inside directory
    remote_fname = posixpath.join(dest_path, test_fname)
    trash_name = remote_fname.replace('/','___')
    JM.remove(remote_fname)
    remote_dir_files = JM.list_files(dest_path)
    trash_files = JM.list_files(JM.trash_dir)
    assert test_fname not in [f['filename'] for f in remote_dir_files]
    assert trash_name in [f['filename'] for f in trash_files]

    # Now remove whole directory
    JM.remove(dest_path)
    trash_name = dest_path.replace('/','___')
    trash_files = JM.list_files(JM.trash_dir)
    assert trash_name in [f['filename'] for f in trash_files]

    # Mock error removing or back-ing up file in trash dir
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'rsync ...', 1,
                            'mock rsync error', '', 'mock error')):
        with pytest.raises(TJMCommandError) as t:
            JM.remove(dest_path)

    # Upload and delete file again (even though file exists already in trash)
    # Should be ok with this.
    JM.upload(test_folder, dest_path)
    JM.remove(dest_path)

    # Empty trash dir and clean local files to conclude tests
    JM.empty_trash()
    _cleanup_local_test_files()


def test_restore():
    """Test restoring file"""

    # Empty trash dir and create a test file and folder
    JM.empty_trash()
    test_fname, test_file, test_folder = _setup_local_test_files()

    # Upload directory with file in it
    dest_dirname = 'test_path'
    dest_path = '/'.join([JM.trash_dir, dest_dirname])
    JM.upload(test_folder, dest_path)

    # Now remove file inside directory and restore it
    remote_fname = posixpath.join(dest_path, test_fname)
    trash_name = remote_fname.replace('/','___')
    JM.remove(remote_fname)
    JM.restore(remote_fname)
    remote_dir_files = JM.list_files(dest_path)
    trash_files = JM.list_files(JM.trash_dir)
    assert test_fname in [f['filename'] for f in remote_dir_files]
    assert trash_name not in [f['filename'] for f in trash_files]

    # Now do the same for the whole directory
    trash_name = dest_path.replace('/','___')
    JM.remove(dest_path)
    JM.restore(dest_path)
    trash_files = JM.list_files(JM.trash_dir)
    assert test_fname in [f['filename'] for f in remote_dir_files]
    assert trash_name not in [f['filename'] for f in trash_files]

    # Mock error error restoring file
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER, 'mv ...', 1,
                            'mock mv error', '', 'mock error')):
        with pytest.raises(TJMCommandError) as t:
            JM.restore(remote_fname)

    # Empty trash dir and clean local files to conclude tests
    JM.empty_trash()
    _cleanup_local_test_files()


def test_empty_trash():
    """Test emptying trash directory"""

    # Empty trash dir and create a test file and folder
    JM.empty_trash()
    test_fname, test_file, test_folder = _setup_local_test_files()

    # Upload directory with file in it to trash directory
    dest_dirname = 'test_path'
    dest_path = '/'.join([JM.trash_dir, dest_dirname])
    JM.upload(test_folder, dest_path)

    # Empty trash directory and assert nothing in it now
    JM.empty_trash()
    trash_files = JM.list_files(JM.trash_dir)
    assert len(trash_files)==0

    # Empty trash dir and clean local files to conclude tests
    JM.empty_trash()
    _cleanup_local_test_files()


def test_write():
    """Test writing file directly"""

    # Empty trash dir
    JM.empty_trash()

    # Write some text data to a file
    txt_file = posixpath.join(JM.trash_dir, 'test.txt')
    JM.write('hello world\n', txt_file)
    trash_files = JM.list_files(JM.trash_dir)
    assert len(trash_files)==1
    assert trash_files[0]['filename']=='test.txt'
    assert trash_files[0]['st_size']==12
    JM.empty_trash()

    # Write some json data to a file
    json_file = posixpath.join(JM.trash_dir, 'test.json')
    JM.write({'msg':'hello world'}, json_file)
    trash_files = JM.list_files(JM.trash_dir)
    assert len(trash_files)==1
    assert trash_files[0]['filename']=='test.json'
    assert trash_files[0]['st_size']==22
    JM.empty_trash()

    # Try to write wrong data type
    with pytest.raises(ValueError):
        JM.write(['hello world'], json_file)

    # Mock not found, permission, and unexpected errors
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=FileNotFoundError('Mock file not found')):
        with pytest.raises(FileNotFoundError):
            JM.write('hello world\n', txt_file)
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=PermissionError('Mock permission error')):
        with pytest.raises(PermissionError):
            JM.write('hello world\n', txt_file)
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=Exception('Unexpected Error')):
        with pytest.raises(Exception):
            JM.write('hello world\n', txt_file)

def test_read():
    """Test reading file directly"""

    # Empty trash dir
    JM.empty_trash()

    txt = 'hello world\n'
    d = {'msg':'hello world'}

    # Write some text and json data to a files
    txt_file = posixpath.join(JM.trash_dir, 'test.txt')
    json_file = posixpath.join(JM.trash_dir, 'test.json')
    JM.write(txt, txt_file)
    JM.write(d, json_file)

    # Read data back in
    txt_data = JM.read(txt_file, data_type='text')
    json_data = JM.read(json_file, data_type='json')
    assert txt_data==txt
    assert json_data==d

    # Try to read not supported data type
    with pytest.raises(ValueError):
        JM.read(txt_file, data_type='list')

    # Mock not found, permission, and unexpected errors
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=FileNotFoundError('Mock file not found')):
        with pytest.raises(FileNotFoundError):
            txt_data = JM.read(txt_file, data_type='text')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=PermissionError('Mock permission error')):
        with pytest.raises(PermissionError):
            txt_data = JM.read(txt_file, data_type='text')
    with patch.object(SSHClient2FA, 'open_sftp',
            side_effect=Exception('Unexpected Error')):
        with pytest.raises(Exception):
            txt_data = JM.read(txt_file, data_type='text')

    # Empty trash dir to finish
    JM.empty_trash()


def test_apps():
    """Test getting and deploying applications"""

    # Name of test app directory locally
    test_app = '.test-app'

    # Remove all apps in apps dir remotely and remove app locally if exists
    JM._execute_command(f"rm -rf {JM.apps_dir}/*")
    os.system(f"rm -rf {test_app}")

    # Create template app locally
    app_config = create_template_app(test_app)

    # Deploy app from files
    app1 = JM.deploy_app(local_app_dir=test_app)
    apps = JM.get_apps()
    assert len(apps)==1
    assert apps[0]==app1['name']

    # Assert app assets, in this case its script, were uploaded
    remote_app_dir = posixpath.join(JM.apps_dir, app1['name'])
    app_files = JM.list_files(remote_app_dir)
    assert 'run.sh' in [f['filename'] for f in app_files]

    # Ge app and assert its config matches what we got back from deploy
    dep_app = JM.get_app(app1['name'])
    assert dep_app==app1

    # Now get app that doesn exist
    with pytest.raises(ValueError):
        bad_app = JM.get_app('bad-app')

    # Now deploy app from dictionary with same name but change node-count
    app1['default_node_count'] = 2*app1['default_node_count']
    app1_up= JM.deploy_app(app_config=app1,
            local_app_dir=test_app, overwrite=True)
    assert app1_up['default_node_count']==2*dep_app['default_node_count']

    # error - overwrite not set
    with pytest.raises(ValueError):
        app1_up= JM.deploy_app(app_config=app1)

    # error - missing app config
    with pytest.raises(ValueError):
        app1.pop('entry_script')
        app1_up= JM.deploy_app(app_config=app1)

    # Force error deploying application data
    with patch.object(TACCJobManager, 'upload',
            side_effect=Exception('Mock upload file error')):
        with pytest.raises(Exception) as e:
            bad_app = JM.deploy_app(local_app_dir=test_app, name='bad_app')

    # Clean app dir to conclude and remove local app
    JM._execute_command(f"rm -rf {JM.apps_dir}/*")
    os.system(f"rm -rf {test_app}")


def test_setup_job():
    """Test setting up jobs"""

    # Name of test app directory locally
    test_app = '.test-app'

    # Remove all apps and jobs  in apps/jobs dir remotely
    JM._execute_command(f"rm -rf {JM.apps_dir}/*")
    JM._execute_command(f"rm -rf {JM.jobs_dir}/*")

    # Remove app locally if exists
    os.system(f"rm -rf {test_app}")

    # Create template app locally
    app_config, job_config = create_template_app(test_app)

    # Deploy app from files
    app1 = JM.deploy_app(local_app_dir=test_app)

    # Setup a test job from the configs in default app directory, dont stage
    job1 = JM.setup_job(local_job_dir=test_app, stage=False)
    assert job1['app']==app_config['name']

    # Setup a test job from dictioanry just loaded, dont stage
    job2 = JM.setup_job(job_config=job1, local_job_dir=test_app, stage=False)
    assert job2['app']==app_config['name']

    # Now create test input file and send with job and stage job
    os.system(f"echo hello world > {job1['inputs']['input1']}")
    job3 = JM.setup_job(job_config=job1.copy(),
            local_job_dir=test_app, stage=True)
    jobs = JM.get_jobs()
    assert job3['job_id'] in jobs
    staged_job = JM.get_job(job3['job_id'])
    assert staged_job==job3

    # Setup another job with allocation and email
    job4 = JM.setup_job(job_config=job1.copy(),
            local_job_dir=test_app, stage=True,
            email='test@test.com', allocation=ALLOCATION)
    jobs = JM.get_jobs()
    assert job4['job_id'] in jobs
    staged_job = JM.get_job(job4['job_id'])
    assert staged_job==job4
    assert 'allocation' in job4.keys() and 'email' in job4.keys()

    # Error - Getting job that doesn't exist
    with pytest.raises(ValueError):
        _ = JM.get_job('does_not_exist')

    # Error - Setting up jobs with missing params/inputs
    no_inputs = job1.copy()
    no_inputs['inputs'] = {}
    no_params = job1.copy()
    no_params['parameters'] = {}
    with pytest.raises(ValueError):
        _ = JM.setup_job(job_config=no_inputs, local_job_dir=test_app, stage=True)
    with pytest.raises(ValueError):
        _ = JM.setup_job(job_config=no_params, local_job_dir=test_app, stage=True)

    # Error - Staging submit script
    with patch.object(TACCJobManager, '_parse_submit_script',
            side_effect=Exception('Mock parse submit script exception')):
        with pytest.raises(Exception):
            _ = JM.setup_job(local_job_dir=test_app, stage=True)

    # Error - Failing to stage input
    with patch.object(TACCJobManager, 'upload',
            side_effect=Exception('Mock upload error')):
        with pytest.raises(Exception):
            _ = JM.setup_job(local_job_dir=test_app, stage=True)

    # Error - Failing to stage app contents (delete job dir)
    _ = JM.remove_job(job3['job_id'])
    with pytest.raises(TJMCommandError):
        _ = JM.setup_job(job_config=job3,
                local_job_dir=test_app, stage=True)

    # cleanup
    JM._execute_command(f"rm -rf {JM.apps_dir}/*")
    JM._execute_command(f"rm -rf {JM.jobs_dir}/*")
    os.system(f"rm -rf {test_app}")


def test_run_jobs():
    """Test submitting and canceling jobs"""

    # Name of test app directory locally
    test_app = '.test-app'

    # Remove all apps and jobs  in apps/jobs dir remotely
    JM._execute_command(f"rm -rf {JM.apps_dir}/*")
    JM._execute_command(f"rm -rf {JM.jobs_dir}/*")

    # Remove app locally if exists
    os.system(f"rm -rf {test_app}")

    # Create template app locally
    app_config, job_config = create_template_app(test_app)

    # Deploy app from files
    app = JM.deploy_app(local_app_dir=test_app)

    # Now create test input file and send with job and stage job
    os.system(f"echo hello world > {job1['inputs']['input1']}")
    job = JM.setup_job(local_job_dir=test_app, stage=True,
            email='test@test.com', allocation=ALLOCATION)

    # Now submit job
    job = JM.submit_job(job4['job_id'])
    assert 'slurm_id' in job4.keys()
    assert job['slurm_id'] in [j['job_id'] for j in JM.showq()]

    # Error - Try submitting again
    with pytest.raises(ValueError):
        _ = JM.submit_job(job['job_id'])

    # Error - Cancel job, mock slurm queue error
    with patch.object(TACCJobManager, '_execute_command',
            side_effect=TJMCommandError(SYSTEM, USER,
                'scancel ...', 1, 'mock scancel error', '', 'mock error')):
        with pytest.raises(TJMCommandError):
            _ = JM.cancel_job(job['job_id'])

    # Cancel job
    job = JM.cancel_job(job['job_id'])
    assert 'slurm_id' not in job.keys()
    assert 'slurm_hist' in job.keys()

    # Error - Try to cancel job again
    with pytest.raises(ValueError):
        _ = JM.cancel_job(job4['job_id'])

    # Error - submit job but mock slurm queue error
    with patch.object(TACCJobManager, '_execute_command', returns='FAILED'):
        with pytest.raises(TJMCommandError):
            _ = JM.submit_job(job['job_id'])

    # cleanup
    JM._execute_command(f"rm -rf {JM.apps_dir}/*")
    JM._execute_command(f"rm -rf {JM.jobs_dir}/*")
    os.system(f"rm -rf {test_app}")


# def test_main(capsys):
#     """CLI Tests"""
#     # capsys is a pytest fixture that allows asserts agains stdout/stderr
#     # https://docs.pytest.org/en/stable/capture.html
#     main(["7"])
#     captured = capsys.readouterr()
#     assert "The 7-th Fibonacci number is 13" in captured.out
