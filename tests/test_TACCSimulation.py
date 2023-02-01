import os
import pdb
from pathlib import Path
from ch_sim.TACCSimulation import TACCSimulation
from unittest.mock import patch
import subprocess

class TestTACCSimulation:

    def test_init(self, test_dir):
        # Mock being on tacc system
        os.environ["HOSTNAME"] = "login.system.tacc.utexas.edu"
        os.environ["SCRATCH"] = str(test_dir)
        simulation = TACCSimulation()
        assert simulation.system == "system"
        assert simulation.jobs_dir == f"{str(test_dir)}/jobs"
        assert simulation.apps_dir == f"{str(test_dir)}/apps"
        assert Path(simulation.jobs_dir).exists()
        assert Path(simulation.apps_dir).exists()

    def test_run_command(self, test_dir):
        # Mock being on tacc system
        os.environ["HOSTNAME"] = "login.system.tacc.utexas.edu"
        os.environ["SCRATCH"] = str(test_dir)
        simulation = TACCSimulation()
        res = simulation.run_command("ls",
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert 'tests\n' in res
        _ = simulation.run_command(f"echo foo > {simulation.jobs_dir}/test_file",
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        res = simulation.run_command(f"ls {simulation.jobs_dir}",
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert 'test_file\n' == res
        pdb.set_trace()

    # The below tests not being on TACC systems, mocking calls to

    def test_tacc_init(self, test_dir):
        with patch('taccjm.taccjm_client.init_jm') as mock_init_jm:
            mock_init_jm.return_value = {'jm_id':'ch-sim-ls6',
                                        'sys':'ls6',
                                        'user':'user',
                                        'apps_dir':f"{str(test_dir)}/apps",
                                        'jobs_dir':f"{str(test_dir)}/jobs"}

            os.environ["HOSTNAME"] = "local.system"
            os.environ["CHSIM_PSW"] = "psw"
            os.environ["CHSIM_USER"] = "user"
            os.environ["CHSIM_SYSTEM"] = "ls6"
            os.environ["CHSIM_ALLOCATION"] = "alloc"
            simulation = TACCSimulation()

            assert simulation.system == 'ls6'
            assert simulation.jm['user'] == 'user'
            assert simulation.jm['jm_id'] == 'ch-sim-ls6'
