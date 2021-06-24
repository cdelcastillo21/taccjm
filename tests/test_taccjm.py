import os
import pytest

from dotenv import load_dotenv
from taccjm.taccjm import JobManager

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# Note: .env file in tests directory must contain TACC_USER and TACC_PW variables defined
load_dotenv()
USER = os.environ.get("TACC_USER")
PW = os.environ.get("TACC_PW")


def test_jm(system, mfa):
    """Integration test for taccjm"""

    # Initialize taccjm that will be used for tests
    s2 = JobManager(system, user=USER, psw=PW, mfa=mfa)

    # Command that should work
    assert s2._execute_command('echo test') == 'test\n'

    # TODO: command that fails and printing commands to stdou. use capsys
    # main(["7"])
    # captured = capsys.readouterr()
    # assert "The 7-th Fibonacci number is 13" in captured.out

    # TODO: Test mkdir ? 

    # TODO: Test send_file (both file and directory, directory with and without hidden files sent)


# def test_fib():
#     """API Tests"""
#   assert fib(1) == 1
#   assert fib(2) == 1
#   assert fib(7) == 13
#   with pytest.raises(AssertionError):
#       fib(-10)
#
#
# def test_main(capsys):
#     """CLI Tests"""
#     # capsys is a pytest fixture that allows asserts agains stdout/stderr
#     # https://docs.pytest.org/en/stable/capture.html
#     main(["7"])
#     captured = capsys.readouterr()
#     assert "The 7-th Fibonacci number is 13" in captured.out
