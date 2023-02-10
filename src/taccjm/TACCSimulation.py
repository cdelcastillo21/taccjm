import pdb
import tempfile
from pathlib import Path
import os
from datetime import datetime
import time

from taccjm.TACCClient import TACCClient
from taccjm.utils import hours_to_runtime_str, get_default_script, init_logger

import __main__


submit_script_template = get_default_script("submit_script.sh", ret="text")
run_script_template = get_default_script("run.sh", ret="text")


def TACCSimulation():
    """
    Base class for a simulation job.
    """

    JOB_DEFAULTS = {
        "allocation": None,
        "node_count": 1,
        "processors_per_node": 48,
        "max_run_time": 0.1,
        "queue": "development",
        "dependencies":  [],
    }

    # These are file/folder inputs needed to run the simulation
    ARGUMENTS = [
            {
                "name": "input_file",
                "type": "input",
                "label": "Input File",
                "desc": "File input to be copied to job dir. " +
                        "Will be passed as argument as well to run()",
            },
            {
                "name": "param",
                "type": "argument",
                "label": "Parameter argument",
                "desc": "Value to be passed to run() method as an argument.",
            }
    ],

    MODULES_LIST = ['remora']

    def __init__(self,
                 name: str = None,
                 log_config: dict = None,
                 ):

        if "__file__" in dir(__main__):
            self.is_script = True
            self.script_file = __main__.__file__
            self.log.info(f"Running from main script {self.script_file}")
        else:
            self.is_script = False
            self.script_file = __file__
            self.log.info(f"Running from non-main script {self.script_file}")

        if name is None:
            self.name = Path(self.script_file).stem

        self.log = init_logger(__name__, log_config)
        self.client = TACCClient(log_config=log_config)
        self.job_config = None

    def _parse_submit_script(self,
                             job_config: dict):
        """
        Parse a job config submit script text, with proper directives.
        """
        extra_directives = ""
        dependency = job_config['slurm'].get("dependency")
        if dependency is not None:
            extra_directives += "\n#SBATCH --dependency=afterok:"+str(dependency)

        rt = job_config['slurm']['max_run_time']
        rt = hours_to_runtime_str(rt) if not isinstance(rt, str) else rt
        txt = submit_script_template.format(
                    job_name=job_config['name'],
                    job_id=job_config['job_id'],
                    job_dir=job_config['job_dir'],
                    module_list=' '.join(MODULES_LIST),
                    allocation=job_config['slurm']['allocation'],
                    queue=job_config['slurm']['queue'],
                    run_time=rt,
                    cores=job_config['slurm']['node_count'] *
                    job_config['slurm']['processors_per_node'],
                    node_count=job_config['slurm']['node_count'],
                    processors_per_node=job_config['slurm']['processors_per_node'],
                    extra_directives=extra_directives,
                    run_cmnd="python3 sim.py",
        )

        return txt

    def setup(
        self,
        args: dict = None,
        slurm_config: dict = None,
        stage: bool = False,
        run: bool = False,
    ) -> dict:
        """
        """
        job_config = {}
        job_config['name'] = self.name
        name = job_config["name"]
        job_config["job_id"] = (
            name
            + "_"
            + datetime.fromtimestamp(time.time()).strftime("%Y%m%d_%H%M%S")
            + next(tempfile._get_candidate_names())
        )
        job_config['job_dir'] = self.client.job_path(job_config["job_id"], '')
        job_config['slurm'] = JOB_DEFAULTS.copy()
        job_config['slurm'].update(slurm_config)
        job_config['args'] = {}

        to_stage = []
        # Process arguments, don't stage yet
        for arg in ARGUMENTS:
            if arg['name'] not in args.keys():
                raise ValueError(f"Missing argument {arg['name']}")
            if arg['type'] == 'input':
                # Argument is passed as path to file in job directory
                job_path = self.client.job_path(job_config['job_id'],
                                                os.path.basename(
                                                  args[arg['name']]))
                job_config['args'][arg['name']] = job_path
                to_stage.append((args[arg['name']], job_path))
            else:
                # Other arguments just passed as their value
                job_config['args'][arg['name']] = args[arg['name']]

        # Parse entry script -> Loads conda env, copies this class and
        # adds appropriate CLI entrypoint for the submit script to execute
        job_config['submit_script'] = self._parse_submit_script(job_config)

        with open(self.script_file, 'r') as fp:
            job_config['script_file'] = fp.read()

        if not stage:
            return job_config

        # Create job directory
        self.client.exec("mkdir {job_config['job_dir']}")

        # Copy job inputs
        for s in to_stage:
            self.client.upload(to_stage[0], to_stage[1])

        # Write submit script
        path = self.client.job_path(job_config['job_id'], 'submit_script.sh')
        self.client.write(job_config['submit_script'], path)
        job_config['submit_script'] = path

        # Write sim.py
        path = self.client.job_path(job_config['job_id'], 'sim.py')
        self.client.write(job_config['submit_script'], path)
        job_config['entry_script'] = path

        # Write job_config
        path = self.client.job_path(job_config['job_id'], 'job.json')
        self.client.write(job_config, path)

        if not run:
            return job_config

        job_config = self.client.submit_job(job_config['job_id'])

        return job_config

    def load_job_env(self):
        """
        Loads the job config from the current execution environment

        TODO: Verify slurm environment variables match job config?
        """
        job_dir = os.getenv('SLURM_SUBMIT_DIR')
        self.job_config = self.client.get_job(job_dir)

    def run(self, config, ibrun=True):
        """Run on HPC resources.

        This is the entry point for a single job within the simulation.
        If not run from a SLURM execution environment, will setup and submit
        the job to be run, returning the job_config.
        """
        self.load_job_config()
        self.log.info("Starting Simulation",
                      extra={'job_config': self.job_config})
        self.client.exec("echo HELLO; pwd; sleep 10; echo GOODBYE")
        self.log.info("Simulation Done")


# Bare minimum main clause necessary to run job
if __name__ == '__main__':
    simulation = TACCSimulation()
    simulation.run()
