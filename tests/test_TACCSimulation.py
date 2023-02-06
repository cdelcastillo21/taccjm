import os
import pdb
from pathlib import Path
from ch_sim.TACCSimulation import TACCSimulation
from unittest.mock import patch
import subprocess
import pytest

@pytest.fixture()
def local_sim(test_dir):
    os.environ["SCRATCH"] = str(test_dir)
    os.environ["HOSTNAME"] = "login.system.tacc.utexas.edu"
    sim = TACCSimulation()
    yield sim

class TestLocalTACCSimulation:
    """
    Test Simulation class if we were on TACC
    """
    @classmethod
    def setup_class(cls):
        pass

    def test_init(self, test_dir, local_sim):
        assert local_sim.system == "system"
        assert local_sim.jobs_dir == f"{str(test_dir)}/jobs"
        assert local_sim.apps_dir == f"{str(test_dir)}/apps"
        assert Path(local_sim.jobs_dir).exists()
        assert Path(local_sim.apps_dir).exists()

    def test_run_command(self, local_sim):
        res = local_sim.run_command("ls", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert 'tests\n' in res
        _ = local_sim.run_command(f"echo foo > {local_sim.jobs_dir}/test_file",
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res = local_sim.run_command(f"ls {local_sim.jobs_dir}",
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert 'test_file\n' == res


# class TestRemoteTACCSimulation:
# 
#     @classmethod
#     def setup_class(self, test_dir):
#         # Mock being on tacc system
#         os.environ["HOSTNAME"] = ""
#         os.environ["SCRATCH"] = str(test_dir)
#         os.environ["CHSIM_PSW"] = "psw"
#         os.environ["CHSIM_USER"] = "user"
#         os.environ["CHSIM_SYSTEM"] = "ls6"
#         os.environ["CHSIM_ALLOCATION"] = "alloc"
#         self.test_dir = str(test_dir)
# 
#     # The below tests not being on TACC systems, mocking calls to
# 
#     def test_tacc_init(self, test_dir):
#         with patch('taccjm.taccjm_client.init_jm') as mock_init_jm:
#             mock_init_jm.return_value = {'jm_id':'ch-sim-ls6',
#                                         'sys':'ls6',
#                                         'user':'user',
#                                         'apps_dir':f"{str(test_dir)}/apps",
#                                         'jobs_dir':f"{str(test_dir)}/jobs"}
# 
#             simulation = TACCSimulation()
#             assert simulation.system == 'ls6'
#             assert simulation.jm['user'] == 'user'
#             assert simulation.jm['jm_id'] == 'ch-sim-ls6'
