import pdb
import tempfile
from pathlib import Path
import os
from datetime import datetime
import time

from taccjm.TACCClient import TACCClient
from taccjm. exceptions import TACCCommandError
from taccjm.utils import (hours_to_runtime_str, get_default_script, init_logger,
                          get_log_level_str, read_log)


import __main__


submit_script_template = get_default_script("submit_script.sh", ret="text")

main_clause = """if __name__ == '__main__':
    import sys
    import logging
    simulation = TACCSimulation(name='{name}',
                                log_config={{'output': {output},
                                            'fmt': '{fmt}',
                                            'level': logging.{level}}})
    simulation.run()"""


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

    ENV_CONFIG = {
        'modules': ['remora'],
        'conda_env': 'taccjm',
        'conda_packages': 'pip',
        'pip_packages': 'git+https://github.com/cdelcastillo21/taccjm.git@0.0.5-improv'
    }

    def __init__(self,
                 name: str = None,
                 system: str = None,
                 log_config: dict = None,
                 ):
        self.log_config, self.log = init_logger(__name__, log_config)

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
                             job_config: dict,
                             run_cmd: str):
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
                    module_list=' '.join(self.ENV_CONFIG['modules']),
                    allocation=job_config['slurm']['allocation'],
                    queue=job_config['slurm']['queue'],
                    run_time=rt,
                    cores=job_config['slurm']['node_count'] *
                    job_config['slurm']['processors_per_node'],
                    node_count=job_config['slurm']['node_count'],
                    processors_per_node=job_config['slurm']['processors_per_node'],
                    extra_directives=extra_directives,
                    run_cmnd=run_cmd,
        )

        return txt

    def _trim_job_dict(self, job_config, max_size=200):
        """
        """
        trimmed = job_config.copy()
        if 'sim_script' in trimmed.keys():
            trimmed['sim_script'] = trimmed['sim_script'][0:200]
        return trimmed

    def _read_log(self):
        """
        Read log if its to a file
        """
        if isinstance(self.log_config['output'], str):
            return read_log(self.log)

    def setup(
        self,
        args: dict = {},
        slurm_config: dict = {},
        python_setup: bool = False,
        stage: bool = False,
        run: bool = False,
        remora: bool = True,
    ) -> dict:
        """
        Set up simulation on TACC
        """
        # Checking execution python environment
        self.log.info('Starting simulation set-up.',
                      extra={'inputs': {'args': args,
                                        'slurm_config': slurm_config,
                                        'stage': stage,
                                        'run': run}})

        # Checking python env set-up
        self.log.info("Setting up python execution environment")
        envs = self.client.get_python_env()
        if not any(envs['name'] == self.ENV_CONFIG['conda_env']):
            python_setup = True
        if python_setup:
            self.client.python_env = self.client.get_install_env(
                self.ENV_CONFIG['conda_env'],
                conda=self.ENV_CONFIG['conda_packages'],
                pip=self.ENV_CONFIG['pip_packages'])

        self.log.info("Creating job config.")
        job_config = {}
        job_config['name'] = self.name
        name = job_config["name"]
        job_config["job_id"] = (
            name
            + "_"
            + datetime.fromtimestamp(time.time()).strftime("%Y%m%d_%H%M%S")
            + next(tempfile._get_candidate_names())
        )
        job_config['job_dir'] = self.client.job_path(job_config["job_id"])
        job_config['slurm'] = self.JOB_DEFAULTS.copy()
        job_config['slurm'].update(slurm_config)

        # Make sure allocation specified
        if job_config['slurm']['allocation'] is None:
            allocations = self.client.get_allocations()
            def_alloc = allocations[0]['name']
            self.log.info(f'Allocation not specified. Using {def_alloc}.',
                          extra={'allocations': allocations})
            job_config['slurm']['allocation'] = def_alloc
        self.log.info("Slurm settings configured",
                      extra={'slurm_config': job_config['slurm']})

        self.log.info("Processing Arguments")
        job_config['args'] = {}
        to_stage = []
        # Process arguments, don't stage yet
        for arg in self.ARGUMENTS:
            if arg['name'] not in args.keys():
                if not arg.get('required', True):
                    continue
                elif 'default' not in arg.keys():
                    msg = f"Missing argument {arg['name']}"
                    self.log.error(msg, extra={'job_args': args})
                    raise ValueError(msg)
                else:
                    self.log.info("Setting arg default value")
                    args[arg['name']] = arg['default']
            self.log.info(f"Found {arg['type']} {arg['name']}",
                          extra={'arg': arg})
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

        run_cmd = f"chmod +x {self.name}.py\n\n"
        if remora:
            run_cmd += f"remora ./{self.name}.py"
        else:
            run_cmd += f"./{self.name}.py"
        # run_cmd += f"{conda_path}/bin/{self.client.pm} run"
        # run_cmd += f" -n {self.ENV_CONFIG['conda_env']} {self.name}.py"
        self.log.info('Parsing submit script',
                      extra={'run_cmd': run_cmd})
        job_config['submit_script'] = self._parse_submit_script(
            job_config,
            run_cmd=run_cmd)

        conda_path = envs['path'][envs['name'] ==
                                  self.ENV_CONFIG['conda_env']].iloc[0]
        self.log.info(f'Reading sim script at {self.script_file}')
        job_config['sim_script'] = f"#!{conda_path}/bin/python\n\n"
        with open(self.script_file, 'r') as fp:
            job_config['sim_script'] += fp.read()
        if "\nif __name__ == '__main__':" not in job_config['sim_script']:
            self.log.info('Detected no main clause... adding.')
            job_config['sim_script'] += "\n\n"
            job_config['sim_script'] += main_clause.format(
                name=self.name,
                output=f"'{self.name}-log.{self.log_config['fmt']}'",
                fmt=self.log_config['fmt'],
                level=get_log_level_str(self.log_config['level']).upper())

        self.log.info('Job configuration done!',
                      extra={'job_config': self._trim_job_dict(job_config)})
        if not stage:
            return job_config

        # Create a temp local job directory
        tmpdir = tempfile.mkdtemp()
        self.log.info(f'Staging job to {tmpdir}',
                      extra={'job_dir': job_config['job_dir']})
        self.client.exec(f"mkdir {job_config['job_dir']}")

        # Copy job inputs to tmpdir
        stage_commands = []
        for s in to_stage:
            tmp_path = str(Path(tmpdir) / os.path.basename(s[0]))
            self.log.info('Staging job input locally',
                          extra={'src': s[0], 'dest': tmp_path})
            stage_commands.append(self.client.upload(s[0], tmp_path,
                                                     wait=False, local=True))

        # Write submit script
        path = str(Path(tmpdir) / 'submit_script.sh')
        script_text = job_config.pop('submit_script')
        self.log.info('Writing submit script', extra={'path': path})
        self.client.write(script_text, path, local=True)
        job_config['submit_script'] = self.client.job_path(
            job_config['job_id'], 'submit_script.sh')

        # Write sim.py
        path = str(Path(tmpdir) / f'{self.name}.py')
        script_text = job_config.pop('sim_script')
        self.log.info('Writing sim script', extra={'path': path})
        self.client.write(script_text, path, local=True)
        job_config['sim_script'] = self.client.job_path(
            job_config['job_id'], f'{self.name}.py')

        # Write job_config
        path = str(Path(tmpdir) / 'job.json')
        self.log.info('Writing job config', extra={'path': path})
        self.client.write(job_config, path, local=True)

        # Wait for stage commands to finish
        self.log.info('Waiting for inputs to finish staging',
                      extra={'stage_commands': stage_commands})
        for s in stage_commands:
            self.client.process(s['id'], local=True,
                                wait=True, error=True)

        # Upload tmpdir to job_path - Non-local operation if necessary
        self.log.info('Uploading job directory', extra={'path': path})
        self.client.upload(tmpdir, '', job_id=job_config['job_id'])

        if not run:
            return job_config

        self.log.info('Submitting job')
        try:
            job_config = self.client.submit_job(job_config['job_id'])
        except TACCCommandError as t:
            self.log.error('Failed to submit job', extra={'err': t})
            raise t
        self.log.info('Job submitted',
                      extra={'slurm_config': job_config['slurm']})

        return job_config

    def run(self,
            args: dict = {},
            slurm_config: dict = {},
            python_setup: bool = False,
            stage: bool = True,
            run: bool = True,
            remora: bool = False,
            ) -> dict:
        """Run on HPC resources.

        This is the entry point for a single job within the simulation.
        If not run from a SLURM execution environment, will setup and submit
        the job to be run, returning the job_config.
        """
        # See if running from within execution environment
        job_dir = os.getenv('SLURM_SUBMIT_DIR')
        if job_dir is None:
            self.log.info('Not in execution environment. Setting up job...')
            job_config = self.setup(args=args,
                                    slurm_config=slurm_config,
                                    stage=stage,
                                    run=run,
                                    python_setup=python_setup,
                                    remora=remora)
            self.log.info("Job {job_config['job_id']} set up and submitted",
                          extra={'job_config': job_config})
            return job_config
        self.job_config = self.client.get_job(job_dir)
        self.log.info('Loaded job config. starting job.',
                      extra={'job_config': self.job_config})
        self.setup_job()
        self.run_job()

    def setup_job(self):
        """
        Command to set-up job directory.

        This is a skeleton method that should be over-written.
        """
        self.log.info("Job set-up Start")
        self.client.exec("sleep 5")
        self.log.info("Job set-up Start")

    def run_job(self):
        """
        Job run entrypoint

        This is a skeleton method that should be over-written.

        Note: ibrun command should be here somewhere.
        """
        input_file = self.job_config['args']['input_file']
        param = self.job_config['args']['param']
        self.log.info("Starting Simulation")
        self.client.exec(f"tail -n {param} {input_file} > out.txt; sleep 10")
        self.log.info("Simulation Done")

