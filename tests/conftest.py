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
from taccjm.utils import create_template_app

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

@pytest.fixture()
def test_app(test_dir):
    # Create template app locally
    configs = create_template_app('test_app', dest_dir=test_dir)
    yield configs
