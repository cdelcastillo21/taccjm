"""
Tests for tacc_ssh_server


"""
import os
import pytest
from pathlib import Path
from unittest.mock import patch
from conftest import bad_transport, good_transport

from fastapi.testclient import TestClient
from taccjm.tacc_ssh_server import app
from taccjm.TACCSSHClient import TACCSSHClient
from paramiko import SSHException

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Create a FastAPI test client instance
client = TestClient(app)

ID = "test"
SYSTEM = "stampede2"
USER = "test_user"
PSW = "test_psw"
MFA = "test_mfa_code"


def test_list_jm():
    # Call the endpoint
    response = client.get("/list")

    # Check the response status code
    assert response.status_code == 200


@patch.object(TACCSSHClient, 'connect')
@patch.object(TACCSSHClient, 'execute_command')
def test_init_jm(connect, execute_command):

    # Call the endpoint
    # response = client.post(f"/{ID}?system={SYSTEM}&user={USER}&psw={PSW}&mfa={MFA}")
    response = client.post(f"/{ID}",
                           json={'system': SYSTEM,
                                 'user': USER,
                                 'psw': PSW,
                                 'mfa': MFA})

    # Check the response status code
    assert response.status_code == 200

    # Check the response content
    response_content = response.json()
    assert response_content["id"] == ID

    # TODO Add more checks


@patch.object(TACCSSHClient, 'process_command')
@patch.object(TACCSSHClient, 'get_transport')
def test_exec(get_transport, process_command):
    """Test executing a command"""

    with patch.object(TACCSSHClient, 'execute_command'):
        with patch.object(TACCSSHClient, 'execute_command'):
            r = client.post(f"/{ID}?system={SYSTEM}&user={USER}&psw={PSW}&mfa={MFA}")

    # Call the endpoint
    get_transport.return_value = bad_transport()
    with pytest.raises(SSHException):
        client.post(f"/{ID}/exec", json={'cmnd': 'pwd', 'wait': True, 'error': True})

    # Command succeeds, just mock the exec_command function in
    get_transport.return_value = good_transport()
    response = client.post(f"/{ID}/exec", json={'cmnd': 'pwd', 'wait': False, 'error': True})

    # Check the response status code
    assert response.status_code == 200

    # Check the response content
    response_content = response.json()
    assert response_content["id"] == 1


@patch.object(TACCSSHClient, 'process_command')
@patch.object(TACCSSHClient, 'get_transport')
def test_process(get_transport, process_command):
    """Test executing a command"""

    with patch.object(TACCSSHClient, 'connect'):
        with patch.object(TACCSSHClient, 'execute_command'):
            client.post(f"/{ID}?system={SYSTEM}&user={USER}&psw={PSW}&mfa={MFA}&restart=1")

    # Call the endpoint
    get_transport.return_value = bad_transport()
    with pytest.raises(SSHException):
        client.post(f"/{ID}/exec", json={'cmnd': 'pwd', 'wait': True, 'error': True})

    # Command succeeds, just mock the exec_command function in
    get_transport.return_value = good_transport()
    response = client.post(f"/{ID}/exec", json={'cmnd': 'pwd', 'wait': False, 'error': True})

    # Check the response status code
    assert response.status_code == 200

    # Check the response content
    response_content = response.json()
    assert response_content["id"] == 1


ex_ls_ret = [{'filename': 'test_file',
   'st_atime': 1675202764,
   'st_gid': 800588,
   'st_mode': 33152,
   'st_mtime': 1674645958,
   'st_size': 98,
   'st_uid': 856065,
   'ls_str': b'-rw-------   1 856065   800588         98 25 Jan 05:25 test_file'},
 {'filename': '.test_dir',
    'st_atime': 1675540759,
    'st_gid': 800588,
    'st_mode': 16832,
    'st_mtime': 1674644754,
    'st_size': 4096,
    'st_uid': 856065,
    'ls_str': b'drwx------   1 856065   800588       4096 25 Jan 05:05 test_dir'}]
@patch.object(TACCSSHClient, 'open_sftp')
def test_list_files(open_sftp):
    """Test executing a command"""

    # Mock init the JM
    with patch.object(TACCSSHClient, 'connect'):
        with patch.object(TACCSSHClient, 'execute_command'):
            client.post(f"/{ID}?system={SYSTEM}&user={USER}&psw={PSW}&mfa={MFA}&restart=1")

    def raise_ssh():
        raise SSHException
    open_sftp.side_effect = raise_ssh
    with pytest.raises(SSHException):
        path = '/foo/bar'
        res = client.get(f"/{ID}/files/{path}")

    # TODO: Figure out how to mocck these calls
