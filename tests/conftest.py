"""
    Dummy conftest.py for taccjm.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""

import pytest
import shutil
from pathlib import Path
from unittest.mock import patch
from taccjm import TACCSSHClient
from paramiko import SSHException

def_test_dir = Path(__file__).parent / ".test_dir"


def pytest_addoption(parser):
    parser.addoption("--mfa", action="store",
            default="012345", help="MFA token. Must be provided")


@pytest.fixture
def mfa(request):
    return request.config.getoption("--mfa")


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
def test_file(test_dir):
    file_path = f'{test_dir}/hello.txt'
    with open(file_path, 'w') as f:
        f.write('Hello World!')
    yield file_path


@pytest.fixture
@patch.object(TACCSSHClient, 'connect')
@patch.object(TACCSSHClient, 'execute_command')
def mocked_client(connect, execute_command):
    client = TACCSSHClient('stampede2', user='test', psw='test', mfa=123456)
    return client


# Command succeeds, just mock the exec_command function in
class good_channel():

    def __init__(self, active=False):
        self.active = active
        pass

    def exec_command(self, cmd):
        pass

    def exit_status_ready(self):
        return True if not self.active else False

    def recv_exit_status(self):
        return 0

    def recv(self, nbytes):
        out = b'test'
        return out[0:nbytes]

    def recv_stderr(self, nbytes):
        return b''

    def close(self):
        pass


class bad_channel(good_channel):

    def recv_stderr(self, nbytes):
        err = b'error'
        return err[0:nbytes]

    def recv_exit_status(self):
        return 1


class good_transport():

    def __init__(self):
        pass

    def open_session(self):
        channel = good_channel()
        return channel


class bad_transport():

    def __init__(self):
        pass

    def open_session(self):
        raise SSHException

