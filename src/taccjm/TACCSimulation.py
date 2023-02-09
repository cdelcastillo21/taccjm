import stat
import pdb
import tempfile
import subprocess
from pathlib import Path
import shutil
import posixpath
import json
import logging
import os
from datetime import datetime
import time
from typing import Union

from taccjm.TACCClient import TACCClient
from taccjm import tacc_ssh_api as tsa
from taccjm.exceptions import TACCJMError
from taccjm.utils import (
    hours_to_runtime_str, get_default_script, init_logger,
    validate_file_attrs, filter_files, stat_all_files_and_folders
    )

import __main__

import click

submit_script_template = get_default_script("submit_script.sh", ret="text")
run_script_template = get_default_script("run.sh", ret="text")


def TACCSimulation():
    """
    Base class for a simulation job.

    This class is to be the main entrypoint for a SLURM job
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

    OUTPUTS = [
          {
                "name": "output",
                "label": "Main output dir",
                "desc": "Directory to save upon completion of job.",
          }
    ]

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
        txt = submit_script_template.format(
                    job_name=job_config["name"],
                    job_id=job_config["job_id"],
                    job_dir=job_config['job_dir'],
                    module_list=' '.join(self.MODULES),
                    allocation=job_config['slurm']['allocation'],
                    queue=job_config['slurm']['queue'],
                    run_time=job_config['slurm']["max_run_time"],
                    cores=job_config['slurm']['node_count'] *
                    job_config['slurm']['processors_per_node'],
                    node_count=job_config['slurm']['node_count'],
                    processors_per_node=job_config['slurm']['processors_per_node'],
                    extra_directives=extra_directives,
        )

        return txt

    def _parse_run_script(self):
        """
        Parse a job run script text, with proper modules loaded.
        """
        if self.modules is not None:
            # TODO: Verify valid modules?
            module_str = "module load " + " ".join(self.modules)
        else:
            module_str = ""
        txt = run_script_template.format(modules_str=module_str)

        return txt

    def _prompt(self, question):
        return input(question + " [y/n]").strip().lower() == "y"

    def setup(
        self,
        args: dict = None,
        slurm_config: dict = None,
        stage: bool = False,
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
        job_config['slurm'] = self.JOB_DEFAULTS.copy()
        job_config['slurm'].update(slurm_config)
        job_config['args'] = {}

        to_stage = []
        # Process arguments, don't stage yet
        for arg in self.ARGUMENTS:
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

        # Parse submit script with SLURM directives and calling entry script
        job_config['entry_script'] = self._parse_run_script(job_config)

        if stage:
            job_config = self.stage(job_config)

        return job_config

    def stage(self,
              job_config):
        """

        """
        # Create temp directory to stage the job
        self.client.exec(

        # Write submit script
        self.client

        # Move temp directory to job location

        # Default in job arguments if they are not specified
        job_config["entry_script"] = _get_attr("entry_script", "entry_script")

        # Copy job inputs 


        if stage:
            # Stage job inputs
            if not any([job_config.get(x) for x in ["job_id", "job_dir"]]):
                # If job_id/job_dir has not been assigned, job hasn't been
                # set up before, so must create job directory.
                ts = datetime.fromtimestamp(time.time()).strftime("%Y%m%d_%H%M%S")
                job_config["job_id"] = "{job_name}_{ts}".format(
                    job_name=job_config["name"], ts=ts
                )
                job_config["job_dir"] = "{job_dir}/{job_id}".format(
                    job_dir=self.jobs_dir, job_id=job_config["job_id"]
                )

                # Make job directory
                self._mkdir(job_config["job_dir"])

            # Utility function to clean-up job directory and raise error
            job_dir = job_config["job_dir"]

            def err(e, msg):
                self._execute_command(f"rm -rf {job_dir}")
                logger.error(f"deploy_job - {msg}")
                raise e

            # Copy app contents to job directory
            cmnd = "cp -r {apps_dir}/{app} {job_dir}/{app}".format(
                apps_dir=self.apps_dir, app=job_config["app"], job_dir=job_dir
            )
            try:
                ret = self._execute_command(cmnd)
            except TJMCommandError as t:
                err(t, "Error copying app assets to job dir")

            # Send job input data to job directory
            if len(job_config["inputs"]) > 0:
                inputs_path = posixpath.join(job_dir, "inputs")
                self._mkdir(inputs_path)
                for arg, path in job_config["inputs"].items():
                    arg_dest_path = posixpath.join(
                        inputs_path, posixpath.basename(path)
                    )
                    try:
                        self.upload(path, arg_dest_path)
                    except Exception as e:
                        err(e, f"Error staging input {arg} with path {path}")

            # Parse and write submit_script to job_directory, and chmod it
            submit_script_path = f"{job_dir}/submit_script.sh"
            try:
                submit_script = self._parse_submit_script(job_config)
                self.write(submit_script, submit_script_path)
                self._execute_command(f"chmod +x {submit_script_path}")
            except Exception as e:
                err(e, f"Error parsing or staging job submit script.")

            # Save job config
            job_config_path = f"{job_dir}/job.json"
            self.write(job_config, job_config_path)

        return job_config


    def setup_simulation(self, **config):
        """Setup the simulation on TACC

        This involves two steps:
            (1) Setting up the 'application' directory for this type of simulation.
            (2) Setting up a job directories, for a executions of this simulation.
            job configs taken generate_job_configs() function.
        Note this function can run locally or on TACC. Depending on where it runs, that
        is where the job configs will be generated, and then they will be pushed
        to TACC once generated.
        """
        self.log.info("Setting up simulation.")

        # setup a TACCJM application directory - This contains the submit scripts
        tmpdir = tempfile.mkdtemp()
        assets_dir = tmpdir + "/assets"
        Path(assets_dir).mkdir(exist_ok=True, parents=True)

        # Copy simulation dependencies.
        remote_deps = []
        if self.deps is not None:
            for d in self.deps:
                if Path(d).exists():
                    shutil.copy(d, assets_dir)
                elif self.jm is not None:
                    remote_deps.append(d)

        # now make the app.json
        app_config = {
            "name": self.name,
            "short_desc": "",
            "long_desc": "",
            "default_queue": config.get("queue", "development"),
            "default_node_count": config.get("nodes", 1),
            "default_processors_per_node": 48,
            "default_max_run_time": "0:30:00",
            "default_memory_per_node": 128,
            "entry_script": "run.sh",
            "inputs": [],
            "parameters": [],
            "outputs": [],
        }
        with open(tmpdir + "/app.json", "w") as fp:
            json.dump(app_config, fp)

        with open(assets_dir + "/run.sh", "w") as fp:
            fp.write(self._parse_run_script())

        # Move app dir
        app_dir = self.apps_dir + "/" + self.name
        if self.jm is not None:
            tjm.upload(self.jm["jm_id"], tmpdir, app_dir)
        else:
            shutil.move(tmpdir, app_dir)
            os.system(f"cp -r {tmpdir} {app_dir}")
        shutil.rmtree(tmpdir)

        job_configs = []
        for job_config in self.generate_job_configs(self, **config):
            updated_job_config = self.deploy_job(job_config)
            job_configs.append(updated_job_config)

        jobs = len(job_configs)
        logger.info("Setup {njobs} jobs.")
        submit = self._prompt(f"Submit {njobs} jobs? [y/n]: ")
        if submit:
            for job_config in job_configs:
                self.submit_job(job_config["job_id"])
        elif self.prompt("Save setup jobs for later?"):
            # TODO - actually save the jobs
            pass
        else:
            for job_config in job_configs:
                self.remove_job(job_config["job_id"])

    def generate_jobs(self, **config):
        """Create the job configs for the simulation.

        Args:
            config - the simulation config
        """
        res = {
            "name": self.name,
            "app": self.name,
            "node_count": config.get("node_count", 1),
            "queue": config.get("queue", "development"),
            "processors_per_node": config.get("processors_per_node", 4),
            "desc": "",
            "inputs": {},
            "parameters": {},
            "dependency": config.get("dependency"),
            "sim_config": self.config,
        }

        if self.allocation is not None:
            res["allocation"] = self.allocation
        if "runtime" in config:
            res["max_run_time"] = hours_to_runtime_str(config["runtime"])

        res["args"] = self.args.__dict__.copy()

        return [self._base_job_config(**config)]

    def run(self, **config):
        """
        Run this simulation according to passed in config


        Set's up a simulation
        """

        # Determine what we need to do
        action = self.args.action
        self.config = config
        if action == "setup":
            # add job dependency id
            config["dependency"] = self.args.dependency
            self.setup(**config)
            return
        elif action == "run":
            self.job_config = self._get_job_config()
            # Ensure simulation-level config is identical to what was set at setup time
            self.config.update(self.job_config.get("sim_config", {}))
            self.setup_job()
            self.run_job()
        else:
            raise ValueError(f"Unsupported action {action}")


    def run_job(self, config, ibrun=True):
        """Run on HPC resources.

        This is the entry point for a single job within the simulation. Must be run
        on a TACC system within a slurm session.
        """
        self.load_config()
        self.log.info("Starting Simulation", extra={'job_config': job_config})
        self.client.exec("echo HELLO; sleep 10; echo GOODBYE")
        self.log.info("Simulation Done")


@click.command()
@click.option('--name', default='My Class', help='The name of the class.')
@click.option('--input_file', default='My Class', help='The name of the class.')
@click.option('--', default='My Class', help='The name of the class.')
def run(name):
    simulation = TACCSimulation(name)
    simulation.run_job()

if __name__ == '__main__':
    run()

