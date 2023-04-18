from mpi4py import MPI
import h5py
import numpy as np
import time
from taccjm.log import logger
from taccjm.sim.TACCSimulation import TACCSimulation


class TestSim(TACCSimulation):

    JOB_DEFAULTS = {
        "allocation": None,
        "node_count": 1,
        "processors_per_node": 48,
        "max_run_time": 0.1,
        "queue": "development",
        "dependencies": [],
    }

    # These are file/folder inputs needed to run the simulation
    ARGUMENTS = [
        {
            "name": "n",
            "type": "argument",
            "label": "N",
            "desc": "Size of array ocreate ",
            "default": 100000000,
        },
        {
            "name": "channels",
            "type": "argument",
            "label": "Channels",
            "desc": "Number of channels to use to write the array.",
            "default": 128,
        },
    ]

    # TODO: Base environment config? for TACC simulation
    BASE_ENV_CONFIG = {
        "modules": ["remora"],
        "conda_packages": "pip",
        "pip_packages": "git+https://github.com/cdelcastillo21/taccjm.git@0.0.5-improv",
    }

    ENV_CONFIG = {
        "conda_env": "taccjm",
        "modules": [],
        "conda_packages": ['pip', 'mpi4py', 'h5py'],
        "pip_packages": [],
    }

    def run_job(self):
        """
        Job run entrypoint

        This is a skeleton method that should be over-written.

        Note: ibrun command should be here somewhere.
        """
        n = self.job_config["args"]["n"]
        channels = self.job_config["args"]["channels"]
        logger.info("Starting Simulation")

        # Use the client.exec function to execute commands.
        # self.client.exec(f"tail -n {param} {input_file} > out.txt; sleep 10")

        logger.info("Simulation Done")

        num_processes = MPI.COMM_WORLD.size
        rank = MPI.COMM_WORLD.rank

        if rank == 0:
            start = time.time()

        np.random.seed(746574366 + rank)

        f = h5py.File('parallel_test.hdf5', 'w', driver='mpio', comm=MPI.COMM_WORLD)
        dset = f.create_dataset('test', (channels, n), dtype='f')

        for i in range(channels):
            if i % num_processes == rank:
                data = np.random.uniform(size=n)
                dset[i] = data

        f.close()

        if rank == 0:
            print('Wallclock Time (s) Elapsed: ' + time.time()-start)
