import stat
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

from taccjm.utils import hours_to_runtime_str, get_default_script, init_logger
from taccjm.utils import validate_file_attrs, filter_files

import __main__

submit_script_template = get_default_script('submit_script.sh', ret='text')
run_script_templatete = get_default_script('run.sh', ret='text')


class TACCSimulation:
    """
    Simulation base class

    A class for setting up a simulation on TACC. This class is environment agnostic.
    And communicates with TACC to set-up and run simulations as necessary. If not
    running on TACC systems, will use taccjm to establish and maintain a connection
    to desired TACC system where simulation is to be run.
    """

    APP_TEMPLATE = {'name': 'base-sim',
                    'short_desc': 'Base TACC Application',
                    'long_desc':  'Base template to build HPC Apps',
                    'default_node_count': 1,
                    'default_processors_per_node': 48,
                    'default_max_run_time': 0.1,
                    'default_queue': 'development',
                    'entry_script': 'run.sh',
                    'inputs': [{'name': 'file',
                                'label': 'Input argument',
                                'desc': 'File input to be copied to job dir.'}],
                    'parameters': [{'name': 'param',
                                    'label': 'Parameter argument',
                                    'desc': 'Value to be parsed into run script'}]}

    def __init__(self,
                 name:str,
                 system:str=None,
                 log=None,
                 logfmt='txt',
                 loglevel=logging.CRITICAL,
                 config:dict=None):
        """
        Upon initializaiton:

        - Determine if running on TACC or not
        - If not running on TACC init TACC Job Manager connection
        - TODO: Tapis support
        """
        self.name = name
        self.commands = {}
        self.log = init_logger(__name__, output=log, fmt=logfmt, loglevel=loglevel)
        self.log.info(f'Logger initialized for {name} with level {loglevel}')

        if '__file__' in dir(__main__):
            self.is_script = True
            self.script_file = __main__.__file__
            self.log.info(f'Running from main script {self.script_file}')
        else:
            self.is_script = False
            self.script_file = __file__
            self.log.info(f'Running from non-main script {self.script_file}')

        if name is None:
            self.name = Path(self.script_file).stem

        # Lets us know if we are waiting on one or more commands to finish before
        # Running a job (for example, set-up scripts still running in background).
        self.blockers = []

        load_dotenv()
        host = os.getenv("HOSTNAME")
        if 'tacc' not in host:
            # Not running on TACC - Initialized job manager to interact with system
            self.log.info('Not running on TACC. Initializiing jm environment')
            self.system = system if system is not None else os.environ.get(
                    "TACCJM_DEFAULT_SYSTEM")
            if self.system is None:
                self.log.critical(''.join(['No system detected in env variable ',
                                           '$TACCJM_DEFAULT_SYSTEM and non passed']))

            self.jm = None
            self.jm_id = f'ch-sim-{self.system}'
            restart = False
            self.log.info(f'Looking for jm {self.jm_id}')
            try:
                self.jm = tjm.get_jm(self.jm_id)
                self.log.info(f'Found jm {self.jm_id}', extra=self.jm)
            except ConnectionError:
                restart = True
                self.log.info('Unnable to connect to jm, restarting')
            except TACCJMError:
                self.log.info(f'Did not find jm {self.jm_id}')
                pass

            if self.jm is None:
                self.log.info(f'Initializing JM {self.jm_id}')
                # TODO: wrap this in try block
                self.jm = tjm.init_jm(self.jm_id,
                                      self.system,
                                      user=os.environ.get("CHSIM_USER"),
                                      psw=os.environ.get("CHSIM_PSW"),
                                      restart=restart)
            self._conda_init()
            self.apps_dir = self.jm['apps_dir']
            self.jobs_dir = self.jm['jobs_dir']
        else:
            # Running on TACC systems - Run things locally
            self.jm = None
            self.system = host.split('.')[1]
            basedir = os.getenv("SCRATCH")
            if basedir is None or not os.path.exists(basedir):
                basedir = os.getenv("WORK")
            self.jobs_dir = basedir + "/jobs"
            self.apps_dir = basedir + "/apps"
            os.makedirs(self.jobs_dir, exist_ok=True)
            os.makedirs(self.apps_dir, exist_ok=True)

    def _conda_init(self,
                    pip='git+https://github.com/cdelcastillo21/taccjm.git@0.0.5-improv'):
        """
        Conda init

        This should only be run from non-TACC systems to verify setup is correct on
        TACC systems. Installs conda (mamba) and setups ch-sim conda environment as
        necessary.
        """
        # Check conda (mamba) installed
        self.log.info('Checking mamba environment')
        res = self.run_command('source ~/.bashrc; mamba activate taccjm; taccjm --help',
                               wait=True, check=False)
        if res['rc'] != 0:
            self.log.info('Did not find conda env, setting up in background.')
            script_path = get_default_script('conda_install.sh')
            tjm.deploy_script(self.jm_id, script_path)
            res = tjm.run_script(self.jm_id, 'conda_install',
                                 args=['taccjm', 'pip', pip],
                                 wait=False)
            self.log.info('conda_init() Running. Setting blocker until completion')
            self.blockers.append(res)
            key = f"{res['name']}_{res['id']}"
            self.commands[key] = res
        else:
            self.log.info('taccjm mamba env found')

    def _prompt(self, question):
        return input(question + " [y/n]").strip().lower() == "y"

    def _check_blockers(self, wait=False):
        """
        """
        blockers = []
        for b in self.blockers:
            cmd_id = f"{b['name']}_{b['id']}"
            cmd = self.get_command(cmd_id, nbytes=None)
            if cmd['status'] == 'COMPLETE':
                self.log.info(f'Blocker {cmd_id} completed')
            elif cmd['status'] == 'FAILED':
                self.log.critical(f'Blocker {cmd_id} failed: {b}')
                raise RuntimeError(f'Blocker {b} failed')
            else:
                self.log.info(f'Blocker {cmd_id} is still running')
                blockers.append(cmd)
        self.blockers = blockers

        if len(self.blockers) == 0:
            return True
        else:
            return False


    def _list_files(self,
                    path='.',
                    attrs=['filename'],
                    hidden:bool=False,
                    search:str=None,
                    match:str=r'.',
                    job_id:str=None) -> List[dict]:
        """
        Get files and file info
        """
        if job_id is not None:
            path = f"{self.jobs_dir}/{job_id}/{path}"
        if self.jm is None:
            # Query path to see if its directory or file
            validate_file_attrs(attrs)
            f_info = []
            f_attrs = ['st_atime', 'st_gid', 'st_mode',
                    'st_mtime', 'st_size', 'st_uid']
            lstat = os.lstat(path)

            if stat.S_ISDIR(lstat.st_mode):
                # If directory get info on all files in directory
                f_attrs.insert(0, 'filename')
                files = sftp.listdir_attr(path)
                for f in files:
                    # Extract fields from SFTPAttributes object for files
                    d = dict([(x, f.__getattribute__(x)) for x in f_attrs])
                    d['ls_str'] = f.asbytes()
                    f_info.append(d)
            else:
                # If file, just get file info
                d = [(x, lstat.__getattribute__(x)) for x in f_attrs]
                d.insert(0, ('filename', path))
                d.append(('ls_str', lstat.asbytes()))
                f_info.append(dict(d))
                files = filter_files(files, attrs=attrs,
                                     hidden=hidden, search=search, match=match)
            return f_info
        else:
            return tjm.list_files(jm_id, path=path, attrs=attrs, hidden=hidden,
                                  search=search, match=match)

    def _read(self,
              path,
              data_type='text',
              job_id:str=None) -> List[dict]:
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            path = f"{self.jobs_dir}/{job_id}/{path}"
        if self.jm is None:
            with open(path, 'r') as fp:
                if data_type == 'json':
                    return json.load(fp)
                else:
                    return fp.read()
        else:
            return tjm.read(self.jm_id, path, data_type=data_type)


    def _write(self, data:, path:str, job_id:str=None) -> None:
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            path = f"{self.jobs_dir}/{job_id}/{path}"
        if self.jm is None:
            if isinstance(data, str):
                with open(path, 'w') as fp:
                    fp.write(data)
            else:
                with open(path, 'w') as fp:
                    json.dump(data, fp)
        else:
            tjm.write(self.jm_id, data, path)

    def run_command(self, cmnd:str='pwd', check=True, wait=True, **kwargs):
        """
        Run Command

        If running on TACC, the commands will be executed using a
        python sub-process. Otherwise, command str will be written to a file (default
        bash shell) and executed remotely
        """
        res = None
        if self.jm is None:
            self.log.info('Running command locally')
            sub_proc = subprocess.run(cmnd, shell=True, check=check, **kwargs)
            if sub_proc.stdout is not None:
                res = sub_proc.stdout.decode('utf-8')
        else:
            self.log.info('Running command remotely via script')
            temp_file = tempfile.NamedTemporaryFile(delete=True)
            temp_file.write(f"#!/bin/bash\n{cmnd}".encode('utf-8'))
            temp_file.flush()
            script_name = Path(temp_file.name).stem
            script_path = str(Path(temp_file.name).resolve())
            self.log.info(f'Deploying script {script_name} at {script_path}')
            tjm.deploy_script(self.jm['jm_id'], script_name, script_path)
            try:
                self.log.info(f'Running script {script_name}')
                res = tjm.run_script(self.jm['jm_id'], script_name, wait=wait)
            except Exception as e:
                msg = f'Command << {cmnd} >> at script file {temp_file.name} failed.'
                if check:
                    self.log.error(msg + '. Check is true, raising error.')
                    raise e
                else:
                    self.log.error(msg + '. Check is false... continuing')
            temp_file.close()

            if wait:
                if res['rc'] != 0:
                    if check:
                        self.log.error('Command error and check is true ')
                        # TODO: Fix this exception
                        raise RuntimeError('Command failed')
                    else:
                        self.log.warning('Command error, check is false, continuing')
                else:
                    self.log.info('Command completed succesfully')
                    # TODO: return whole dictionary or stdout only?
                    # res = res['stdout']
            if not wait:
                key = f"{res['name']}_{res['id']}"
                self.commands[key] = res
                res = key

        return res

    def get_command(self, cmd_id:str, nbytes=100):
        """
        Wait for given command to finish executing
        """
        if cmd_id not in self.commands:
            raise ValueError('Invalid command ID cmd_id')
        else:
            try:
                res = tjm.get_script_status(self.jm_id,
                                            self.commands[cmd_id]['id'],
                                            nbytes=nbytes)
            except Exception as t:
                self.log.critical(f'Error getting script {cmd_id} status')
                raise t
            self.commands[cmd_id] = res

        return res

    def get_apps(self) -> List[str]:
        """
        Get list of applications deployed by TACCJobManager instance.

        Parameters
        ----------
        None

        Returns
        -------
        apps : list of str
            List of applications deployed.

        """
        apps = self._list_files(path=self.apps_dir,
                                attrs=['filename', 'st_mode'],
                                hidden=False)
        apps = [a for a in apps if stat.S_ISDIR(a['st_mode'])]

        return apps


    def get_app(self, app_id:str) -> dict:
        """
        Get application config for app deployed at TACCJobManager.apps_dir.

        Parameters
        ----------
        app_id : str
            Name of app to pull config for.
        ----------

        Returns
        -------
        app_config : dict
            Application config dictionary as stored in application directory.

        Raises
        ------
        ValueError
            If app_id does not exist in applications folder.

        """
        # Get current apps already deployed
        cur_apps = self.get_apps()
        if app_id not in cur_apps:
            msg = f"get_app - Application {app_id} does not exist."
            self.log.error(msg)
            raise ValueError(msg)

        # Load application config
        app_config_path = '/'.join([self.apps_dir, app_id, 'app.json'])
        app_config = self._read(path, data_type='json')

        return app_config


    def deploy_app(self,
            app_config:dict=None,
            local_app_dir:str='.',
            app_config_file:str="app.json",
            overwrite:bool=False,
            **kwargs) -> dict:
        """
        Deploy local application code associated with this simulation. Values in project
        config file are substituted in where needed in the app config file to
        form application config, and then app contents in assets directory
        (relative to local_app_dir) are sent to to the apps_dir along with the
        application config (as a json file).

        Parameters
        ----------
        app_config : dict, default=None
            Dictionary containing app config. If None specified, then app
            config will be read from file specified at
            local_app_dir/app_config_file.
        local_app_dir: str, default='.'
            Directory containing application to deploy.
        app_config_file: str, default='app.json'
            Path relative to local_app_dir containing app config json file.
        overwrite: bool, default=False
            Whether to overwrite application if it already exists in
            application directory.
        **kwargs : dict, optional
            All extra keyword arguments will be interpreted as items to
            override in app config found in json file.

        Returns
        -------
        app_config : dict
            Application config dictionary as stored in application directory.

        Raises
        ------
        ValueError
            If app_config is missing a required field or application already
            exists but overwrite is not set to True.
        """
        # Implement overwrite check?
        # Only overwrite previous version of app if overwrite is set.
        # cur_apps = self.get_apps()
        # if (app_config['name'] in cur_apps) and (not overwrite):
        #     msg = f"deploy_app - {app_config['name']} already exists."
        #     logger.info(msg)
        #     raise ValueError(msg)

        # Make entry point script executable
        entry_script = f"{deploy_app_dir}/{app_config['entry_script']}"
        self._run_command(f"chmod +x {entry_script}")

        # setup a TACCJM application directory - This contains the submit scripts
        tmpdir = tempfile.mkdtemp()
        assets_dir = tmpdir + "/assets"
        Path(assets_dir).mkdir(exist_ok=True, parents=True)

        # If running from a main script file, then patch up

        # TODO: Copy simulation dependencies - Implement later
        # remote_deps = []
        # if self.deps is not None:
        #     for d in self.deps:
        #         if Path(d).exists():
        #             shutil.copy(d, assets_dir)
        #         elif self.jm is not None:
        #             remote_deps.append(d)

        # Load templated app configuration
        app_config = self.APP_TEMPLATE

        # Update with kwargs
        app_config.update(**kwargs)

        # Check for valid app config?
        # missing = set(self.APP_TEMPLATE.keys()) - set(app_config.keys())
        # if len(missing)>0:
        #     msg = f"deploy_app - missing required app configs {missing}"
        #     logger.error(msg)
        #     raise ValueError(msg)


        # Write config file
        with open(tmpdir + "/app.json", "w") as fp:
            json.dump(app_config, fp)

        # Write entrypoint
        with open(assets_dir + "/run.sh", "w") as fp:
            fp.write(self._parse_run_script())

        # Move app dir
        local_app_dir = os.path.join(local_app_dir, 'assets')
        deploy_app_dir = self.apps_dir + "/" + self.name
        if self.jm is None:
            self.upload(local_app_dir, deploy_app_dir)
        else:
            shutil.copytree(local_app_dir, deploy_app_dir)

        return app_config

    def get_jobs(self) -> List[str]:
        """
        Get list of all jobs in TACCJobManager jobs directory.

        Parameters
        ----------
        None

        Returns
        -------
        jobs : list of str
            List of jobs contained deployed.

        """
        jobs = self._list_files(path=self.jobs_dir,
                                attrs=['filename', 'st_mode'],
                                hidden=False)
        jobs = [j for j in jobs if stat.S_ISDIR(a['st_mode'])]

        return jobs


    def get_job(self, job_id:str) -> dict:
        """
        Get job config for job in TACCJobManager jobs_dir.

        Parameters
        ----------
        job_id : str
            ID of job to get.
        ----------

        Returns
        -------
        job_config : dict
            Job config dictionary as stored in json file in job directory.

        Raises
        ------
        ValueError
            If invalid job ID (job does not exist).

        """
        try:
            job_config_path = '/'.join([self.jobs_dir, job_id, 'job.json'])
            return self._read(job_config_path, data_type='json')
        except FileNotFoundError as e:
            # Invalid job ID because job doesn't exist
            msg = f"get_job - {job_id} does not exist."
            logger.error(msg)
            raise ValueError(msg)


    def deploy_job(self,
            job_config:dict=None,
            local_job_dir:str='.',
            job_config_file:str='job.json',
            stage:bool=True,
            **kwargs) -> dict:
        """
        Setup job directory on supercomputing resources. If job_config is not
        specified, then it is parsed from the json file found at
        local_job_dir/job_config_file. In either case, values found in
        dictionary or in parsed json file can be overrided by passing keyword
        arguments. Note for dictionary values, only the specific keys in the
        dictionary value specified will be overwritten in the existing
        dictionary value, not the whole dictionary.

        Parameters
        ----------
        job_config : dict, default=None
            Dictionary containing job config. If None specified, then job
            config will be read from file at local_job_dir/job_config_file.
        local_job_dir : str, default='.'
            Local directory containing job config file and project config file.
            Defaults to current working directory.
        job_config_file : str, default='job.json'
            Path, relative to local_job_dir, to job config json file. File
            only read if job_config dictionary not given.
        stage : bool, default=False
            If set to True, stage job directory by creating it, moving
            application contents, moving job inputs, and writing submit_script
            to remote system.
        kwargs : dict, optional
            All extra keyword arguments will be used as job config overrides.

        Returns
        -------
        job_config : dict
            Dictionary containing info about job that was set-up. If stage
            was set to True, then a successful completion of deploy_job()
            indicates that the job directory was prepared succesffuly and job
            is ready to be submit.

        Raises
        ------
        """
        # Load from json file if job conf dictionary isn't specified
        if job_config is None:
            job_config = self._read(local_job_dir / job_config_file,
                                    data_type='json')

        # Overwrite job_config loaded with kwargs keyword arguments if specified
        job_config.update(**kwargs)

        # Get default arguments from deployed application
        app_config = self.get_app(job_config['app'])
        def _get_attr(j, a):
            # Helper function to get app defatults for job configs
            if j in job_config.keys():
                return job_config[j]
            else:
                return app_config[a]

        # Default in job arguments if they are not specified
        job_config['entry_script'] = _get_attr('entry_script', 'entry_script')
        job_config['desc'] = _get_attr('desc','short_desc')
        job_config['queue'] = _get_attr('queue','default_queue')
        job_config['node_count'] = int(_get_attr('node_count',
                'default_node_count'))
        job_config['processors_per_node'] = int(_get_attr('processors_per_node',
                'default_processors_per_node'))
        job_config['max_run_time'] = _get_attr('max_run_time',
                'default_max_run_time')

        # Verify appropriate inputs and arguments are passed
        for i in app_config['inputs']:
            if i['name'] not in job_config['inputs']:
                raise ValueError(f"deploy_job - missing input {i['name']}")
        for i in app_config['parameters']:
            if i['name'] not in job_config['parameters']:
                raise ValueError(f"deploy_job - missing parameter {i['name']}")

        if stage:
            # Stage job inputs
            if not any([job_config.get(x) for x in ['job_id', 'job_dir']]):
                # If job_id/job_dir has not been assigned, job hasn't been
                # set up before, so must create job directory.
                ts = datetime.fromtimestamp(
                        time.time()).strftime('%Y%m%d_%H%M%S')
                job_config['job_id'] = '{job_name}_{ts}'.format(
                        job_name=job_config['name'], ts=ts)
                job_config['job_dir'] = '{job_dir}/{job_id}'.format(
                        job_dir=self.jobs_dir, job_id=job_config['job_id'])

                # Make job directory
                self._mkdir(job_config['job_dir'])

            # Utility function to clean-up job directory and raise error
            job_dir = job_config['job_dir']
            def err(e, msg):
                self._execute_command(f"rm -rf {job_dir}")
                logger.error(f"deploy_job - {msg}")
                raise e

            # Copy app contents to job directory
            cmnd = 'cp -r {apps_dir}/{app} {job_dir}/{app}'.format(
                    apps_dir=self.apps_dir,
                    app=job_config['app'],
                    job_dir=job_dir)
            try:
                ret = self._execute_command(cmnd)
            except TJMCommandError as t:
                err(t, "Error copying app assets to job dir")

            # Send job input data to job directory
            if len(job_config['inputs'])>0:
                inputs_path = posixpath.join(job_dir, 'inputs')
                self._mkdir(inputs_path)
                for arg, path in job_config['inputs'].items():
                    arg_dest_path = posixpath.join(inputs_path,
                            posixpath.basename(path))
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


    def submit_job(self, job_id:str) -> dict:
        """
        Submit job to remote system job queue.

        Parameters
        ----------
        job_id : str
            ID of job to submit.

        Returns
        -------
        job_config : dict
            Dictionary containing information on job just submitted.
            The new field 'slurm_id' should be added populated with id of job.

        Raises
        ------
        """
        # Load job config
        job_config =  self.get_job(job_id)

        # Check if this job isn't currently in the queue
        if 'slurm_id' in job_config.keys():
            msg = f"submit_job - {job_id} exists : {job_config['slurm_id']}"
            raise ValueError(msg)

        # Submit to SLURM queue -> Note we do this from the job_directory
        cmnd = f"cd {job_config['job_dir']}; "
        cmnd += f"sbatch {job_config['job_dir']}/submit_script.sh"
        ret = self.run_command(cmnd)
        if '\nFAILED\n' in ret:
            raise TJMCommandError(self.system, self.user, cmnd, 0, '', ret,
                                  f"submit_job - SLURM error")
        job_config['slurm_id'] = ret.split('\n')[-2].split(' ')[-1]

        # Save job config
        job_config_path = job_config['job_dir'] + '/job.json'
        self.write(job_config, job_config_path)

        return job_config


    def cancel_job(self, job_id:str) -> dict:
        """
        Cancel job on remote system job queue.

        Parameters
        ----------
        job_id : str
            ID of job to submit.

        Returns
        -------
        job_config : dict
            Dictionary containing information on job just canceled. The field
            'slurm_id' should be removed from the job_config dictionary and the
            'slurm_hist' field should be populated with a list of previous
            slurm_id's with the latest one appended. Updated job config is
            also updated in the jobs directory.
        """
        # Load job config
        job_config =  self.get_job(job_id)

        if 'slurm_id' in job_config.keys():
            cmnd = f"scancel {job_config['slurm_id']}"
            try:
                self._execute_command(cmnd)
            except TJMCommandError as e:
                e.message= f"cancel_job - Failed to cancel job {job_id}."
                logger.error(e.message)
                raise e

            # Remove slurm ID and store into job hist
            job_config['slurm_hist'] = job_config.get('slurm_hist',[])
            job_config['slurm_hist'].append(job_config.pop('slurm_id'))

            # Save updated job config
            job_config_path = job_config['job_dir'] + '/job.json'
            self.write(job_config, job_config_path)
        else:
            msg = f"Job {job_id} has not been submitted yet."
            logger.error(msg)
            raise ValueError(msg)

        return job_config


    def remove_job(self, job_id:str) -> str:
        """
        Remove Job

        Cancels job if it has been submitted to the job queue and deletes the
        job's directory. Note job can be restored with restore() command called
        on jobs directory.

        Parameters
        ----------
        job_id : str
            Job ID of job to clean up.

        Returns
        -------
        job_id : str
            ID of job just removed.
        """
        # Cancel job, if needed.
        try:
            self.cancel_job(job_id)
        except:
            pass

        # Remove job directory, if it still exists
        job_dir = '/'.join([self.jobs_dir, job_id])
        try:
            self.remove(job_dir)
        except:
            pass

        return job_id


    def restore_job(self, job_id:str) -> dict:
        """
        Restores a job that has been previously removed (sent to trash).

        Parameters
        ----------
        job_id : str
            ID of job to restore

        Returns
        -------
        job_config : dict
            Config of job that has just been restored.

        Raises
        ------
        ValueError
            If job cannot be restored because it hasn't been removed or does
            not exist.
        """
        # Unix paths -> Get file remote file name and directory
        job_dir = '/'.join([self.jobs_dir, job_id])

        try:
            self.restore(job_dir)
        except FileNotFoundError as f:
            msg = f"restore_job - Job {job_id} cannot be restored."
            logger.error(msg)
            raise ValueError(msg)

        # Return restored job config
        job_config = self.get_job(job_id)

        return job_config

    def download_job_file(self, job_id:str,
            path:str, dest_dir:str='.', file_filter:str='*') -> str:
        """
        Download file/folder at path, relative to job directory, and place it
        in the specified local destination directory. Note downloaded job data
        will always be placed within a folder named according to the jobs id.

        Parameters
        ----------
        job_id : str
            ID of job.
        path : str
            Path to file/folder, relative to the job directory jobs_dir/job_id,
            to download. If a folder is specified, contents are compressed,
            sent, and then decompressed locally.
        dest_dir :  str, optional
            Local directory to download job data to. Defaults to current
            working directory. Note data will be placed within folder with name
            according to the job's id, within the destination directory.


        Returns
        -------
        dest_path : str
            Local path of file/folder downloaded.
        """
        # Downlaod to local job dir
        path = path[:-1] if path[-1]=='/' else path
        fname = '/'.join(path.split('/')[-1:])
        job_folder = '/'.join(path.split('/')[:-1])

        # Make local data directory if it doesn't exist already
        local_data_dir = os.path.join(dest_dir, job_id)
        os.makedirs(local_data_dir, exist_ok=True)

        # Get file
        src_path = '/'.join([self.jobs_dir, job_id, path])
        dest_path = os.path.join(local_data_dir, fname)
        try:
            self.download(src_path, dest_path, file_filter=file_filter)
        except Exception as e:
            m = f"download_job_file - Unable to download {src_path}"
            logger.error(m)
            raise e
        return dest_path


    def upload_job_file(self, job_id:str, path:str,
            dest_dir:str='.', file_filter:str='*') -> None:
        """
        Upload file/folder at local `path` to `dest_dir`, relative to job
        directory on remote TACC system.

        Parameters
        ----------
        job_id : str
            ID of job.
        path : str
            Path to local file or folder to send to job directory.
        dest_dir: str
            Destination unix-style path, relative to `job_id`'s job directory,
            of file/folder. If a file is being sent, remote is the destination
            path. If a folder is being sent, remote is the folder where the
            file contents will go. Note that if path not absolute, then it's
            relative to user's home directory.
        file_filter: str, optional, Default = '*'
            If a folder is being uploaded, unix style pattern matching string
            to use on files to upload within folder. For example, '*.txt'
            would only upload .txt files.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.

        Warnings
        --------
        Will overwrite existing files and folders/subfolders. If job is
        currently executing, then this could disrupt job behavior, be careful!
        """
        try:
            # Get destination directory in job path to send file to
            fname = os.path.basename(os.path.normpath(path))
            dest_path = '/'.join([self.jobs_dir, job_id, dest_dir, fname])

            self.upload(path, dest_path, file_filter=file_filter)
        except Exception as e:
            msg = f"upload_job_file - Unable to upload {path} to {dest_path}."
            logger.error(msg)
            raise e
        return dest_path


    def peak_job_file(self, job_id:str,
            path:str, head:int=-1, tail:int=-1) -> str:
        """
        Performs head/tail on file at given path to "peak" at job file.

        Parameters
        ----------
        path : str
            Unix-style path, relative to job directory.
        head : int, optional, default=-1
            If greater than 0, the number of lines to get from top of file.
        tail : int, optional, default=-1
            If greater than 0, the number of lines to get from bottom of file.
            Note: if head is specified, then tail is ignored.

        Returns
        -------
        txt : str
            Raw text from job file.

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.
        """
        # Load job config
        path = '/'.join([self.jobs_dir, job_id, path])

        try:
            return self.peak_file(path, head=head, tail=tail)
        except Exception as e:
            msg = f"peak_job_file - Unable to peak at file {path}."
            logger.error(msg)
            raise e

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

    def generate_jobs(self,
                      **config):
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


    def setup_job(self):
        """Called before the main work is done for a job
        """
        self._run_command("mkdir outputs")
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

    def make_main_command(self, cmd="echo dummy-command; sleep 10"):
        """Make main command"""
        return "ibrun " + cmd

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

    def run_job(self):
        """Run on HPC resources.

        This is the entry point for a single job within the simulation. Must be run
        on a TACC system within a slurm session.
        """

        run = self.config
        run_dir = self.job_config['job_dir']
        cmd = self.make_main_command
        pre_cmd = self.make_preprocess_command(run, run_dir)
        post_cmd = self.make_postprocess_command(run, run_dir)
        if pre_cmd is not None:
            self._run_command(pre_cmd, check=False)

        self._run_command("ibrun " + self.make_main_command(run, run_dir))
        
        if post_cmd is not None:
            self._run_command(post_cmd)

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

