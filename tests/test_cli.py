"""
Tests for taccjm

TODO: Pytest fixture for clean test-taccjm on remote system

"""
import os
import pdb
import json
import stat
import pytest
import posixpath
from dotenv import load_dotenv
from pathlib import Path
import shutil
from click.testing import CliRunner

from taccjm.cli.cli import cli

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

def_test_dir = Path(__file__).parent / ".test_dir"

@pytest.fixture()
def test_dir():
    if def_test_dir.exists():
        shutil.rmtree(def_test_dir)
    def_test_dir.mkdir(exist_ok=True)
    test_dir_path = Path(def_test_dir).absolute()
    yield test_dir_path
    try:
        shutil.rmtree(test_dir_path)
    except:
        pass

@pytest.fixture()
def test_script(test_dir):
    script_path = str(test_dir.absolute() / 'test_script.sh')
    with open(script_path, 'w') as fp:
        fp.write('#!/bin/bash\nsleep 5\necho foo\n')
    yield script_path

@pytest.fixture()
def test_file(test_dir):
    file_path = f'{test_dir}/hello.txt'
    with open(file_path, 'w') as f:
        f.write('Hello World!')
    yield file_path

def pytest_addoption(parser):
    parser.addoption("--mfa", action="store",
            default="012345", help="MFA token. Must be provided")

@pytest.fixture
def mfa(request):
    return request.config.getoption("--mfa")

def test_init(mfa):
  runner = CliRunner()
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'init', TEST_JM_ID, SYSTEM, USER, '--mfa', mfa])
  assert result.exit_code == 0

def test_find_server():
  runner = CliRunner()
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'find-server'])
  assert result.exit_code == 0

def test_list():
  runner = CliRunner()
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'list'])
  assert result.exit_code == 0

def test_allocations():
  runner = CliRunner()
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'allocations'])
  assert result.exit_code == 0

def test_queue():
  runner = CliRunner()
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'queue'])
  assert result.exit_code == 0

def test_scripts(test_script):
  runner = CliRunner()
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'scripts',
                               '--jm_id', TEST_JM_ID,
                               'deploy', f'{test_script}',
                               '--rename', 'foo'])
  assert result.exit_code == 0
  assert len(result.output) == 45 # exactly one in list, the one we deployed
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'scripts',
                               '--jm_id', TEST_JM_ID,
                               'deploy', f'{test_script}'])
  assert result.exit_code == 0
  assert len(result.output) == 80 # exactly one in list, the one we deployed
  result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'scripts',
                               '--jm_id', TEST_JM_ID,
                               'list',
                               '--match', 'foo|test_script'])
  assert result.exit_code == 0
  assert len(result.output) == 96 # exactly one in list, the one we deployed
  result1 = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'scripts',
                               '--jm_id', TEST_JM_ID,
                               'run', 'foo'])
  assert result1.exit_code == 0
  result2 = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                               'scripts',
                               '--jm_id', TEST_JM_ID,
                               'run', f'{Path(test_script).stem}'])
  assert result2.exit_code == 0
  assert result1.output == result2.output

def test_files(test_file, test_dir):
    runner = CliRunner()
    dirname = str(Path(test_dir).name)
    fname = str(Path(test_file).name)
    download_path = f'{test_dir}/{Path(test_file).stem}-down.txt'

    # Remove. Don't care about result
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'remove', fname])
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'list', '--match', fname])
    assert result.exit_code == 0
    assert len(result.output) == 216
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'upload', test_file, fname])
    assert result.exit_code == 0
    assert len(result.output) == 290

    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'peak', fname])
    assert result.exit_code == 0
    assert len(result.output) == 60

    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'download', fname, download_path])
    assert result.exit_code == 0
    with open(test_file, 'r') as f1, open(download_path, 'r') as f2:
        assert f1.read() == f2.read()

    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'write', fname, 'goodbye!'])
    assert len(result.output) == 290
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'read', fname])
    assert result.exit_code == 0
    assert len(result.output) == 9

    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'remove', fname])
    assert result.exit_code == 0
    assert fname not in result.output
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'files',
                                 '--jm_id', TEST_JM_ID,
                                 'restore', fname])
    assert result.exit_code == 0
    assert fname in result.output

def test_apps(test_dir):
    runner = CliRunner()
    app_name = 'test-app'
    app_dir = str((Path(test_dir) / app_name).absolute())

    # Create template
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'apps',
                                 'template', app_name, '--dest_dir', test_dir])
    assert result.exit_code == 0

    # Deploy app
    deploy_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'apps',
                                 'deploy', app_dir, '-o'])
    assert deploy_result.exit_code == 0
    assert len(deploy_result.output) == 1853

    # TODO: Bug - this should fail but it doesn't
    # Bad deploy - no overwrite
    # deploy_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
    #                              'apps',
    #                              'deploy', app_dir])
    # assert deploy_result.exit_code == 1

    # Make sure app appears in list
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'apps',
                                 'list', '--match', app_name])
    assert result.exit_code == 0
    assert len(result.output) == 65

    # Get ppplication config
    get_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'apps',
                                 'get', app_name])
    assert get_result.exit_code == 0
    assert app_name in result.output

def test_jobs(test_file, test_dir):
    runner = CliRunner()
    app_name = 'test-app'
    job_name = f'{app_name}-test-job'
    app_dir = str((Path(test_dir) / app_name).absolute())
    job_config_path = str((Path(test_dir) / app_name / 'job.json').absolute())

    # Create template app
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'apps',
                                 'template', app_name, '--dest_dir', test_dir])
    assert result.exit_code == 0

    # Edit job config input file
    jc = {}
    with open(job_config_path, 'r') as fp:
        jc = json.load(fp)
    jc['inputs']['input1'] = job_config_path
    jc['allocation'] = ALLOCATION
    with open(job_config_path, 'w') as fp:
        json.dump(jc, fp)

    # Deploy app
    result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'apps',
                                 'deploy', app_dir, '-o'])
    assert result.exit_code == 0

    deploy_result = runner.invoke(cli,
                                  ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                   'jobs',
                                   'deploy', '--config_file', job_config_path])
    assert deploy_result.exit_code == 0

    list_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'list', '--match', job_name])
    assert list_result.exit_code == 0
    job_id = list_result.output.split('\n')[3][2:-2]
    assert job_id.startswith('test-app-test-job')

    submit_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'submit', job_id])
    assert submit_result.exit_code == 0

    cancel_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'cancel', job_id])
    assert cancel_result.exit_code == 0

    remove_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'remove', job_id])
    assert remove_result.exit_code == 0

    list_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'list', '--match', job_name])
    assert list_result.exit_code == 0
    assert job_id not in list_result.output

    restore_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'restore', job_id])
    assert restore_result.exit_code == 0

    list_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'list', '--match', job_name])
    assert list_result.exit_code == 0
    assert job_id in list_result.output

    remove_result = runner.invoke(cli, ['--server', 'localhost', str(TEST_TACCJM_PORT),
                                 'jobs',
                                 'remove', job_id])
    assert remove_result.exit_code == 0


