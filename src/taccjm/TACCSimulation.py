import pdb
import tempfile
from taccjm import taccjm_client as tjm
from taccjm.exceptions import TACCJMError
import subprocess
from pathlib import Path
import shutil
import json
import logging
import os
from datetime import datetime
import time
from dotenv import load_dotenv

from ch_sim.utils import hours_to_runtime_str

logger = logging.getLogger(__name__)

submit_script_template = """#!/bin/bash
#----------------------------------------------------
# {job_name} 
#
#----------------------------------------------------

#SBATCH -J {job_id}     # Job name
#SBATCH -o {job_dir}/{job_id}.o%j # Name of stdout output file
#SBATCH -e {job_dir}/{job_id}.e%j # Name of stderr error file
#SBATCH -p {queue}      # Queue (partition) name
#SBATCH -N {node_count}          # Total num nodes
#SBATCH -n {cores}          # Total num mpi tasks
#SBATCH -t {run_time}         # Run time (hh:mm:ss)
#SBATCH --ntasks-per-node {processors_per_node} # tasks per node
#SBATCH -A {allocation} # Allocation name
{extra_directives}
#----------------------------------------------------

cd {job_dir}


export NP={cores}
{job_dir}/run.sh
"""

run_script_template = """#!/bin/bash

source ~/.bashrc

conda activate ch-sim
{module_str}
python3 sim.py --action=run
"""


class TACCSimulation:
    """
    Simulation base class

    A class for setting up a simulation on TACC. If running on TACC, this class serves
    as the entry point to set-up and run a simulation. If running locally, will ensure
    package is installed on TACC systems, and will set-up/run the simulation remotely.
    """

    REQUIRED_PARAMS = {}

    def __init__(self, name:str, system:str=None, config:dict=None):
        """
        Upon initializaiton:

        - Determine if running on TACC or not
        - If not running on TACC init TACC Job Manager connection
        - TODO: Tapis support
        """
        load_dotenv()

        import __main__

        self.name = name
        if '__file__' in dir(__main__):
            self.script_file = script_file = __main__.__file__
            if name is None:
                self.name = Path(script_file).stem

        host = os.getenv("HOSTNAME")
        if 'tacc' not in host:
            # Not running on TACC
            self.system = system if system is not None else os.environ.get("CHSIM_SYSTEM")
            self.jm_id = f'ch-sim-{self.system}'
            restart = False
            try:
                jms = [j for j in tjm.list_jms() if j['jm_id'] == self.jm_id]
            except ConnectionError:
                jms = []
                restart = True

            if len(jms) > 0:
                self.jm = jms[0]
            else:
                self.jm = tjm.init_jm(self.jm_id,
                                      self.system,
                                      user=os.environ.get("CHSIM_USER"),
                                      psw=os.environ.get("CHSIM_PSW"),
                                      restart=restart)
            self._conda_init()
            self.system = self.jm['sys']
            self.apps_dir = self.jm['apps_dir']
            self.jobs_dir = self.jm['jobs_dir']
        else:
            # Running on TACC systems
            self.jm = None
            self.system = host.split('.')[1]
            basedir = os.getenv("SCRATCH")
            if basedir is None or not os.path.exists(basedir):
                basedir = os.getenv("WORK")
            self.jobs_dir = basedir + "/jobs"
            self.apps_dir = basedir + "/apps"
            os.makedirs(self.jobs_dir, exist_ok=True)
            os.makedirs(self.apps_dir, exist_ok=True)

    def _conda_init(self):
        """
        Conda init

        This should only be run from non-TACC systems to verify setup is correct on
        TACC systems. Installs conda (mamba) and setups ch-sim conda environment as
        necessary.
        """
        # Check conda (mamba) installed
        exe = "Mambaforge-pypy3-Linux-x86_64.sh"
        lnk = f"https://github.com/conda-forge/miniforge/releases/latest/download/{exe}"
        try:
            self.run_command('mamba env list')
        except TACCJMError as e:
            if 'command not found' in str(e):
                cmnd = "; ".join([f"wget -q -P $WORK {lnk}",
                                  f"chmod +x $WORK/{exe}",
                                  f"$WORK/{exe} -b -p $WORK/mambaforge",
                                  "$WORK/mambaforge/bin/conda init",
                                  "$WORK/mambaforge/bin/mamba init"])
                print("Installing conda, this may take a while...")
                _ = self.run_command(cmnd, wait=False)
            else:
                print(f'Unknown error initializing conda env {str(e)}')
                raise e

        # Check conda (mamba) installed
        try:
            pdb.set_trace()
            self.run_command('conda activate ch-sim')
        except TACCJMError as e:
            if 'conda: ' in str(e):
                cmnd = "conda create -n ch-sim taccjm "
                print("Creating ch-sim conda env, this may take a while...")
                _ = self.run_command(cmnd)
            else:
                print(f'Unknown error activating conda env {str(e)}')
                raise e


    def _parse_submit_script(self, job_config):
        """
        Parse a job config submit script text, with proper directives.
        """
        extra_directives = ""
        dependency = job_config.get("dependency")
        if dependency is not None:
            extra_directives += "\n#SBATCH --dependency=afterok:"+str(dependency)
        txt = submit_script_template.format(
                    job_name=job_config["name"],
                    job_id=job_config["job_id"],
                    job_dir=job_config['job_dir'],
                    allocation=job_config["allocation"],
                    queue=job_config["queue"],
                    run_time=job_config["max_run_time"],
                    cores=job_config["node_count"]*job_config["processors_per_node"],
                    node_count=job_config["node_count"],
                    processors_per_node=job_config["processors_per_node"],
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

    def run_command(self, cmnd:str='pwd', check=True, wait=True, **kwargs):
        """
        Run Command

        If running on TACC, the commands will be executed using a
        python sub-process. Otherwise, command str will be written to a file (default
        bash shell) and executed remotely
        """
        res = None
        if self.jm is None:
            sub_proc = subprocess.run(cmnd, shell=True, check=check, **kwargs)
            if sub_proc.stdout is not None:
                res = sub_proc.stdout.decode('utf-8')
        else:
            temp_file = tempfile.NamedTemporaryFile(delete=True)
            temp_file.write(f"#!/bin/bash\n{cmnd}".encode('utf-8'))
            temp_file.flush()
            tjm.deploy_script(self.jm['jm_id'], temp_file.name)
            script_name = Path(temp_file.name).stem
            try:
                res = tjm.run_script(self.jm['jm_id'], script_name, wait=wait)
            except Exception as e:
                print(f'Command {cmnd} at script file {temp_file.name} failed.')
                raise e
            temp_file.close()

        return res

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
        logger.info("Setting up simulation.")

        # setup a TACCJM application directory - This contains the submit scripts
        tmpdir = tempfile.mkdtemp()
        assets_dir = tmpdir + "/assets"
        p = Path(assets_dir)
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(exist_ok=True, parents=True)
        shutil.copy(self.script_file, assets_dir + "/sim.py")

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
            tjm.upload(self.jm['jm_id'], tmpdir, app_dir)
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

    def generate_job_configs(self, **config):
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
            "sim_config": self.config
        }

        if self.allocation is not None:
            res["allocation"] = self.allocation
        if "runtime" in config:
            res["max_run_time"] = hours_to_runtime_str(config["runtime"])

        res["args"] = self.args.__dict__.copy()

        return [self._base_job_config(**config)]


    def deploy_job(self, job_config):
        """
        Deploy job


        Given a valid job config dictionary, deploy a simulation job onto TACC.
        Deployment here means setting up a job directory on TACC fro a simulation.
        If this function is running not running on TACC, job folders are generated
        locally and then moved to TACC. 
        TACC system.
        """
        name = job_config["name"]
        job_config["job_id"] = name + "_" + datetime.fromtimestamp(
                        time.time()).strftime('%Y%m%d_%H%M%S') + \
            next(tempfile._get_candidate_names())
        job_config["job_dir"] = self.jobs_dir + "/" + job_config["job_dir"]
        app_dir = self.apps_dir + "/" + job_config["app"]
        submit_script_text = self._parse_submit_script(job_config)

        if self.jm is not None:
            pass
        else:
            os.system(f"cp {app_dir}/* {self['job_dir']}")
            with open(self['job_dir'] + "/job.json", "w") as fp:
                json.dump(job_config, fp)
            fname = job_config['job_dir'] + "/submit_script.sh"
            with open(fname, "w") as fp:
                fp.write(submit_script_text)

        return job_config

    def submit_job(self, job_id):
        job_dir = self.jobs_dir + "/" + job_id
        os.chmod(f"{job_dir}/run.sh", 0o700)
        print("Submitting job in ", job_dir)
        os.system(f"sbatch {job_dir}/submit_script.sh")

    def remove_job(self, job_config):
        job_dir = job_config['job_dir']
        shutil.rmtree(job_dir)

    ## Functions bellow here are to be execute from within a SLURM job on tacc
    @click.command()
    def run(self, **config):
        """Either setup the simulation/submit jobs OR run on TACC resources.
        """

        # Determine what we need to do
        action = self.args.action
        self.config = config
        if action == "setup":
            # add job dependency id
            config["dependency"] = self.args.dependency
            self._validate_config()
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

    def setup_job(self):
        """Called before the main work is done for a job
        """

        exec_name = self._get_exec_name()
        os.makedirs("inputs", exist_ok=True)
	# if there are directories in the inputs dir, cp will still copy the inputs,
	# but will have a non-zero exit code
        self._run_command(f"cp {self.config['inputs_dir']}/* inputs", check=False)
        self._run_command("ln -sf inputs/* .")
        self._run_command(
            f"cp {self.config['execs_dir']}/" + "{adcprep," + exec_name + "} ."
        )
        self._run_command(f"chmod +x adcprep {exec_name}")

    def run_job(self):
        """Run on HPC resources.

        This is the entry point for a single job within the simulation. Must be run
        on a TACC system within a slurm session.
        """

        run = self.config
        run_dir = self.job_config['job_dir']
        pre_cmd = 
        post_cmd = self.make_postprocess_command(run, run_dir)
        if pre_cmd is not None:
            self._run_command(pre_cmd, check=False)

        self._run_command("ibrun " + self.make_main_command(run, run_dir))
        
        if post_cmd is not None:
            self._run_command(post_cmd)

    def make_preprocess_command(self, run, run_dir):
        writers, workers = self.get_writers_and_workers()
        job_dir = self.job_config['job_dir']
        return ";".join(
            [
                f"cd {run_dir}",
                f"printf '{workers}\\n1\\nfort.14\\n' | {job_dir}/adcprep > {run_dir}/adcprep.log",
                f"printf '{workers}\\n2\\n' | {job_dir}/adcprep >> {run_dir}/adcprep.log",
                f"cd {job_dir}",
            ]
        )

    def make_main_command(self, run, run_dir):
        job_dir = self.job_config["job_dir"]
        writers, workers = self.get_writers_and_workers()
        exec_name = self._get_exec_name()
        if job_dir != run_dir:
            return f"{job_dir}/{exec_name} -I {run_dir} -O {run_dir} -W {writers}"
        else:
            return f"{job_dir}/{exec_name} -W {writers}"

    def make_postprocess_command(self, run, run_dir):
        outdir = run.get('outputs_dir')
        if outdir is not None:
            os.makedirs(outdir, exist_ok=True)
            command = "cp"
            if run.get("symlink_outputs"):
                command = "ln -sf"
            return f"{command} {run_dir}/*.nc {outdir}"

    def _get_args(self):
        action_parser = ap.ArgumentParser(add_help=False)
        action_parser.add_argument("--action", default="setup", choices=["setup", "run"])
        # optional job dependency
        action_parser.add_argument("--dependency", type=str)
        action_args, _ = action_parser.parse_known_args()
        if action_args.action == "setup":
            parser = ap.ArgumentParser(parents=[action_parser])
            self.add_commandline_args(parser)
            args = parser.parse_args()
            args.action = action_args.action
            return args
        else:
            return action_args


    def add_commandline_args(self, parser):
        pass

    def get_arg(self, arg):
        return self.job_config["args"][arg]

    def _format_param(self, param):
        if type(param) is str:
            return param.format(**self.job_config["args"])
        return param

    def _get_job_config(self):
        """Get config of local job
        """

        with open("job.json", "r") as fp:
            return json.load(fp)

    def config_by_host(self, **kwargs):
        import socket
        hostname = socket.gethostname()
        for pattern, config in kwargs.items():
            if pattern in hostname:
                return config

        raise ValueError("Unsupported hostname '{hostname}', must contain one of {list(kwargs.keys)}.") 


    @staticmethod
    def hours_to_runtime_str(hours):
        days = math.floor(hours / 24)
        hours = hours - 24 * days
        minutes = int(60 * (hours - math.floor(hours)))
        hours = int(hours)
        if days:
            return f"{days}-{hours:02}:{minutes:02}:00"
        else:
            return f"{hours:02}:{minutes:02}:00"

  
    def get_writers_and_workers(self):
        node_count = int(self.config.get("node_count"))
        procsPerNode = int(self.job_config.get("processors_per_node"))        
        totalProcs = node_count * procsPerNode
        writers = node_count
        workers = totalProcs - writers
        return writers, workers
