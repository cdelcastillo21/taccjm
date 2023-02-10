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


class TACCSimulation():
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
    ]

    MODULES_LIST = ['remora']

    def __init__(self,
                 name: str = None,
                 system: str = None,
                 log_config: dict = None,
                 ):
        self.log = init_logger(__name__, log_config)

        if "__file__" in dir(__main__):
            self.is_script = True
            self.script_file = __main__.__file__
            self.log.info(f"Running from main script {self.script_file}")
        else:
            self.is_script = False
            self.script_file = __file__
            self.log.info(f"Running from non-main script {self.script_file}")

        self.name = name if name is not None else Path(self.script_file).stem
        self.client = TACCClient(system=system, log_config=log_config)
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
                    module_list=' '.join(self.MODULES_LIST),
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
        args: dict = {},
        slurm_config: dict = {},
        stage: bool = False,
        run: bool = False,
    ) -> dict:
        """
        Set up simulation on TACC
        """
        self.log.info('Starting simulation set-up',)
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
        job_config['slurm'] = self.JOB_DEFAULTS.copy()
        job_config['slurm'].update(slurm_config)
        self.log.info('Slurm config set. Processing args',
                      extra={'slurm_config': job_config['slurm']})

        # Make sure allocation specified
        if job_config['slurm']['allocation'] is None:
            allocations = self.client.get_allocations()
            def_alloc = allocations[0]['name']
            self.log.info(f'Allocation not specified. Using {def_alloc}.',
                          extra={'allocations': allocations})
            job_config['slurm']['allocation'] = def_alloc
        job_config['args'] = {}

        to_stage = []
        # Process arguments, don't stage yet
        for arg in self.ARGUMENTS:
            if arg['name'] not in args.keys():
                msg = f"Missing argument {arg['name']}"
                self.log.error(msg, extra={'args': args})
                raise ValueError(msg)
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
            job_config['sim_script'] = fp.read()

        if not stage:
            return job_config

        # Create job directory
        self.client.exec(f"mkdir {job_config['job_dir']}")

        # Copy job inputs
        for s in to_stage:
            self.client.upload(s[0], s[1])

        # Write submit script
        path = self.client.job_path(job_config['job_id'], 'submit_script.sh')
        script_text = job_config.pop('submit_script')
        self.client.write(script_text, path)
        job_config['submit_script'] = path

        # Write sim.py
        path = self.client.job_path(job_config['job_id'], 'sim.py')
        script_text = job_config.pop('sim_script')
        self.client.write(script_text, path)
        job_config['sim_script'] = path

        # Write job_config
        path = self.client.job_path(job_config['job_id'], 'job.json')
        self.client.write(job_config, path)

        if not run:
            return job_config

        job_config = self.client.submit_job(job_config['job_id'])

        return job_config

    def run(self,
            args: dict = None,
            slurm_config: dict = None):
        """Run on HPC resources.

        This is the entry point for a single job within the simulation.
        If not run from a SLURM execution environment, will setup and submit
        the job to be run, returning the job_config.
        """
        # See if running from within execution environment
        job_dir = os.getenv('SLURM_SUBMIT_DIR')
        if job_dir is None:
            self.log.info('Not in execution environment. Setting up job...')
            job_config = self.setup(args=args, slurm_config=slurm_config,
                                    stage=True, run=True)
            self.log.info("Job {job_config['job_id']} set up and submitted",
                          extra={'job_config': job_config})
            return job_config
        self.job_config = self.client.get_job(job_dir)
        self.log.info('Loaded job config. starting job.',
                      extra={'job_config': job_config})
        self.run_job()

    def run_job(self):
        """
        Job run entrypoint
        """
        input_file = self.job_config['args']['input_file']
        param = self.job_config['args']['param']
        self.log.info("Starting Simulation")
        self.client.exec(f"tail -n {param} {input_file}")
        self.log.info("Simulation Done")


# Bare minimum main clause necessary to run job
if __name__ == '__main__':
    simulation = TACCSimulation()
    simulation.run()
