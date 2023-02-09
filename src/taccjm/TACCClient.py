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
from typing import Union, List

from taccjm import tacc_ssh_api as tsa
from taccjm.exceptions import TACCJMError
from taccjm.utils import (
    hours_to_runtime_str, get_default_script, init_logger,
    validate_file_attrs, filter_files, stat_all_files_and_folders
    )

submit_script_template = get_default_script("submit_script.sh", ret="text")
run_script_templatete = get_default_script("run.sh", ret="text")


class TACCClient:
    """
    TACC Environment Manager

    A class for setting up a simulation on TACC. This class is environment agnostic.
    And communicates with TACC to set-up and run simulations as necessary. If not
    running on TACC systems, will use taccjm to establish and maintain a connection
    to desired TACC system where simulation is to be run.
    """

    CONDA_REQUIRED = "pip"
    PIP_REQUIRED = "git+https://github.com/cdelcastillo21/taccjm.git@0.0.5-improv"

    def __init__(
        self,
        system: str = None,
        log_config: dict = None
    ):
        """
        Upon initializaiton:

        - Determine if running on TACC or not
        - If not running on TACC init TACC Job Manager connection
        - TODO: Tapis support
        """
        self.commands = {}
        self.log = init_logger(__name__, log_config)

        host = os.getenv("HOSTNAME")
        self.local = True
        if "tacc" not in host:
            # Not running on TACC - Initialized job manager to interact with system
            self.local = False
            self.log.info("Not running on TACC. Starting SSH session.")
            self.system = (
                system
                if system is not None
                else os.environ.get("TACCJM_DEFAULT_SYSTEM")
            )
            if self.system is None:
                self.log.critical(
                    "".join(
                        [
                            "No system detected in env variable ",
                            "$TACCJM_DEFAULT_SYSTEM and non passed",
                        ]
                    )
                )

            self.ssh_client = None
            self.ssh_id = f"taccjm-{self.system}"
            restart = False
            self.log.info(f"Looking for ssh_connection {self.ssh_id}")
            try:
                self.ssh_client = tsa.get(self.ssh_id)
                self.log.info(f"Found ssh session {self.ssh_id}",
                              extra=self.ssh_client)
            except ConnectionError:
                restart = True
                self.log.info("Unnable to connect to ssh session, restarting")
            except TACCJMError:
                self.log.info(f"{self.ssh_id}, does not exist. Creating.")

            if self.ssh_client is None:
                self.log.info(f"Initializing ssh session {self.ssh_id}")
                # TODO: wrap this in try block
                self.ssh_client = tsa.init(
                    self.ssh_id,
                    self.system,
                    user=os.environ.get("CHSIM_USER"),
                    psw=os.environ.get("CHSIM_PSW"),
                    restart=restart,
                )

            # TODO: Implement conda-setup for simulation running
            # self._conda_init()
            self.scratch_dir = self.get_env_var('SCRATCH')
            self.home_dir = self.get_env_var('HOME')
            self.work_dir = self.get_env_var('WORK')
        else:
            # Running on TACC systems - Run things locally
            self.ssh_client = None
            self.system = host.split(".")[1]
            self.scratch_dir = os.getenv("SCRATCH")
            self.work_dir = os.getenv("WORK")
            self.home_dir = os.getenv("HOME")

        self.apps_dir = posixpath.join(self.work_dir, f'{self.ssh_id}/apps')
        self.scripts_dir = posixpath.join(self.work_dir, f'{self.ssh_id}/scripts')
        self.jobs_dir = posixpath.join(self.scratch_dir, f'{self.ssh_id}/jobs')

    def get_env_var(self, var: str):
        """
        Resolve an environment variable
        """
        return tsa.exec(self.ssh_id, f'echo ${var}')['stdout'].strip()

    def abspath(self, path):
        """
        Makes remote path absolute if it is not. Relative paths are assumed to
        be relative to a user's scratch_dir.
        """
        if self.local:
            path = str(Path(path).resolve())
        else:
            path = path if posixpath.isabs(path) else posixpath.join(
                self.scratch_dir, path)
        return path

    def job_dir(self, job_id):
        """ Get job dir given job_id """
        return posixpath.join(self.jobs_dir, job_id)

    def job_path(self, job_id, path):
        """ Get job dir given job_id """
        return posixpath.join(self.jobs_dir, job_id, path)

    def list_files(
        self,
        path=".",
        attrs=["filename",
               "st_atime",
               "st_gid",
               "st_mode",
               "st_mtime",
               "st_size",
               "st_uid",
               "ls_str"],
        recurse: bool  = False,
        hidden: bool = False,
        search: str = None,
        match: str = r".",
        job_id: str = None,
    ):
        """
        Get files and file info
        """
        if job_id is not None:
            if posixpath.isabs(path):
                raise ValueError('Path must be relative if job_id specified.')
            path = self.job_path(job_id, path)
        else:
            path = self.abspath(path)

        if self.ssh_client is None:
            return stat_all_files_and_folders(path)
        else:
            return tsa.list_files(
                self.ssh_id,
                path=path,
                attrs=attrs,
                recurse=recurse,
                hidden=hidden,
                search=search,
                match=match,
            )

    def read(self, path, data_type="text", job_id: str = None):
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            path = self.job_path(job_id, path)
        if self.ssh_client is None:
            with open(path, "r") as fp:
                if data_type == "json":
                    return json.load(fp)
                else:
                    return fp.read()
        else:
            return tsa.read(self.ssh_id, path, data_type=data_type)

    def write(self, data, path: str, job_id: str = None) -> None:
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            path = self.job_path(job_id, path)
        if self.ssh_client is None:
            if isinstance(data, str):
                with open(path, "w") as fp:
                    fp.write(data)
            else:
                with open(path, "w") as fp:
                    json.dump(data, fp)
        else:
            tsa.write(self.ssh_id, data, path)

    def exec(self, cmnd: str = "pwd", wait=True, error=True):
        """
        Run Command

        If running on TACC, the commands will be executed using a
        python sub-process. Otherwise, command str will be written to a file
        (default bash shell) and executed remotely
        """
        cmnd_config = None
        if self.ssh_client is None:
            self.log.info("Running command locally",
                          extra={'cmnd': cmnd, 'wait': wait, 'error': error})
            sub_proc = subprocess.Popen(cmnd,
                                        shell=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
            command_id = len(self.commands)
            cmnd_config = {
                "id": command_id,
                "cmd": cmnd,
                "ts": datetime.now(),
                "status": "STARTED",
                "stdout": "",
                "stderr": "",
                "history": [],
                "process": sub_proc,
                "rt": None
            }
        else:
            cmnd_config = tsa.exec(self.ssh_id, cmnd, wait=wait)

        self.commands[str(cmnd_config['id'])] = cmnd_config

        if wait:
            return self.process(cmnd_config['id'], wait=wait, error=error)
        else:
            return cmnd_config

    def process(self, cmnd_id, wait=True, error=True, nbytes=None):
        """
        Poll an executed command to see if it has completed.
        """
        if self.ssh_client is None:
            self.log.info(f'Getting command {cmnd_id} from local command list')
            cmnd_config = self.commands[str(cmnd_id)]
            prev_status = {'status': cmnd_config['status'],
                           'ts': cmnd_config['ts']}
            cmnd_config['rc'] = cmnd_config['process'].poll()
            if wait or cmnd_config['rc'] is not None:
                self.log.info(f"Waiting for command {cmnd_id} to finish....")
                ts = datetime.now()
                cmnd_config['history'].append(prev_status)
                cmnd_config['stdout'] = cmnd_config['process'].stdout.read()
                cmnd_config['stderr'] = cmnd_config['process'].stderr.read()
                cmnd_config['ts'] = ts
                cmnd_config['rt'] = (
                    cmnd_config['ts'] - cmnd_config['history'][-1]['ts']
                    ).seconds
                if cmnd_config['rc'] != 0:
                    cmnd_config['status'] == 'FAILED'
                else:
                    cmnd_config['status'] == 'COMPLETE'
            else:
                self.log.info(f"Command {cmnd_id} is still running.")
                cmnd_config['stdout'] = cmnd_config['process'].stdout.read()
                cmnd_config['status'] = 'RUNNING'
                cmnd_config['ts'] = datetime.now()
                if cmnd_config['status'] != prev_status['status']:
                    cmnd_config['history'].append(prev_status)
        else:
            cmnd_config = tsa.process(self.ssh_id, cmnd_id,
                                      nbytes=nbytes, wait=wait)
            self.commands[str(cmnd_config['id'])] = cmnd_config

        if cmnd_config['status'] == 'FAILED':
            msg = f'Command failed: {cmnd_config}'
            self.log.error(msg, extra={'cmnd_config': cmnd_config})
            if error:
                raise RuntimeError(msg)

        return cmnd_config

    def remove(self, remote_path: str) -> None:
        """
        'Removes' a file/folder by moving it to the trash directory. Trash
        should be emptied out preiodically with `empty_trash()` method.
        Can also restore file `restore(path)` method.

        Parameters
        ----------
        remote_path : str
            Unix-style path for the file/folder to send to trash. Relative to
            home directory for user on TACC system if not an absolute path.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.
        PermissionError
            If user does not have permission to modify specified remote path
        TJMCommandError
            If a directory is being sent, this error is thrown if there are any
            issues unpacking the sent .tar.gz file in the destination directory.

        """
        # Unix paths -> Get file remote file name and directory
        remote_path = self.abspath(remote_path)
        file_name = remote_path.replace("/", "___")
        trash_path = f"{self.trash_dir}/{file_name}"
        cmnd = f"rsync -a {remote_path} {trash_path} && rm -rf {abs_path}"
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = f"remove - Unable to remove {remote_path}"
            logger.error(tjm_error.message)
            raise tjm_error

    def restore(self, remote_path: str) -> None:
        """
        Restores a file/folder from the trash directory by moving it back to
        its original path.

        Parameters
        ----------
        remote_path : str
            Unix-style path of the file/folder that should not exist anymore to
            restore.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If the file to be restored does not exist in trash directory.
        TJMCommandError
            If error moving data from trash to its original path. This could
            be becasue the original path still has data in it/exists.
        """
        # Unix paths -> Get file remote file name and directory
        file_name = remote_path.replace("/", "___")
        trash_path = posixpath.join(self.trash_dir, file_name)
        abs_path = "./" + remote_path if remote_path[0] != "/" else remote_path

        # Check if trash path is a file or directory
        is_dir = False
        try:
            with self._client.open_sftp() as sftp:
                fileattr = sftp.stat(trash_path)
                is_dir = stat.S_ISDIR(fileattr.st_mode)
        except FileNotFoundError as f:
            msg = f"restore - file/folder {file_name} not in trash."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)

        src_path = f"{trash_path}/" if is_dir else trash_path
        cmnd = f"rsync -a {src_path} {abs_path} && rm -rf {trash_path}"
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = f"restore - Unable to restore {remote_path}"
            logger.error(tjm_error.message)
            raise tjm_error

    def empty_trash(self, filter_str: str = "*") -> None:
        """
        Cleans out trahs directly by permently removing contents with rm -rf
        command.

        Parameters
        ----------
        filter : str, default='*'
            Filter files in trash directory to remove

        Returns
        -------

        """
        self._execute_command(f"rm -rf {self.trash_dir}/{filter_str}")


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
        jobs = self.list_files(
            path=self.jobs_dir, attrs=["filename", "st_mode"], hidden=False
        )
        jobs = [j for j in jobs if stat.S_ISDIR(j["st_mode"])]

        return jobs

    def get_job(self, job_id: str) -> dict:
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
        job_config_path = self.job_path('job_id', 'job.json')
        try:
            job_config = self.read(job_config_path, data_type="json")
        except FileNotFoundError:
            # Invalid job ID because job doesn't exist
            msg = f"get_job - {job_id} does not exist."
            self.log.error(msg)
            raise ValueError(msg)

        return job_config

    def submit_job(self, job_id: str) -> dict:
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
        job_config = self.get_job(job_id)

        # Check if this job isn't currently in the queue
        if "slurm_id" in job_config.keys():
            msg = f"submit_job - {job_id} exists : {job_config['slurm_id']}"
            raise ValueError(msg)

        # Submit to SLURM queue -> Note we do this from the job_directory
        cmnd = f"cd {job_config['job_dir']}; "
        cmnd += f"sbatch {job_config['job_dir']}/submit_script.sh"
        ret = self.run_command(cmnd)
        if "\nFAILED\n" in ret:
            raise TJMCommandError(
                self.system, self.user, cmnd, 0, "", ret, f"submit_job - SLURM error"
            )
        job_config["slurm_id"] = ret.split("\n")[-2].split(" ")[-1]

        # Save job config
        job_config_path = job_config["job_dir"] + "/job.json"
        self.write(job_config, job_config_path)

        return job_config

    def cancel_job(self, job_id: str) -> dict:
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
        job_config = self.get_job(job_id)

        if "slurm_id" in job_config.keys():
            cmnd = f"scancel {job_config['slurm_id']}"
            try:
                self._execute_command(cmnd)
            except TJMCommandError as e:
                e.message = f"cancel_job - Failed to cancel job {job_id}."
                logger.error(e.message)
                raise e

            # Remove slurm ID and store into job hist
            job_config["slurm_hist"] = job_config.get("slurm_hist", [])
            job_config["slurm_hist"].append(job_config.pop("slurm_id"))

            # Save updated job config
            job_config_path = job_config["job_dir"] + "/job.json"
            self.write(job_config, job_config_path)
        else:
            msg = f"Job {job_id} has not been submitted yet."
            logger.error(msg)
            raise ValueError(msg)

        return job_config

    def remove_job(self, job_id: str) -> str:
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
        except Exception:
            pass

        # Remove job directory, if it still exists
        job_dir = self.job_dir(job_id)
        try:
            self.remove(job_dir)
        except Exception:
            pass

        return job_id

    def restore_job(self, job_id: str) -> dict:
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
        job_dir = "/".join([self.jobs_dir, job_id])

        try:
            self.restore(job_dir)
        except FileNotFoundError as f:
            msg = f"restore_job - Job {job_id} cannot be restored."
            logger.error(msg)
            raise ValueError(msg)

        # Return restored job config
        job_config = self.get_job(job_id)

        return job_config

#     def get_apps(self):
#         """
#         Get list of applications deployed by TACCJobManager instance.
# 
#         Parameters
#         ----------
#         None
# 
#         Returns
#         -------
#         apps : list of str
#             List of applications deployed.
# 
#         """
#         apps = self.list_files(
#             path=self.apps_dir, attrs=["filename", "st_mode"], hidden=False
#         )
#         apps = [a for a in apps if stat.S_ISDIR(a["st_mode"])]
# 
#         return apps
# 
#     def get_app(self, app_id: str):
#         """
#         Get application config for app deployed at TACCJobManager.apps_dir.
# 
#         Parameters
#         ----------
#         app_id : str
#             Name of app to pull config for.
#         ----------
# 
#         Returns
#         -------
#         app_config : dict
#             Application config dictionary as stored in application directory.
# 
#         Raises
#         ------
#         ValueError
#             If app_id does not exist in applications folder.
# 
#         """
#         # Get current apps already deployed
#         cur_apps = self.get_apps()
#         if app_id not in cur_apps:
#             msg = f"get_app - Application {app_id} does not exist."
#             self.log.error(msg)
#             raise ValueError(msg)
# 
#         # Load application config
#         app_config = self.read(
#             posixpath.join(self.apps_dir, app_id, "app.json"))
# 
#         return app_config


# 
#     def _conda_install(self, conda=CONDA_REQUIRED, pip=PIP_REQUIRED):
#         """
#         Conda install
# 
#         This should only be run from non-TACC systems to verify setup is correct on
#         TACC systems. Installs conda (mamba) and setups ch-sim conda environment as
#         necessary.
#         """
#         # Check conda (mamba) installed
#         self.log.info("Checking mamba environment")
#         res = self.run_command(
#             "source ~/.bashrc; mamba activate taccjm; taccjm --help",
#             wait=True,
#             check=False,
#         )
#         if res["rc"] != 0:
#             self.log.info("Did not find conda env, setting up in background.")
#             script_path = get_default_script("conda_install.sh")
#             tjm.deploy_script(self.jm_id, script_path)
#             res = tjm.run_script(
#                 self.jm_id, "conda_install", args=["taccjm", "pip", pip], wait=False
#             )
#             self.log.info("conda_init() Running. Setting blocker until completion")
#             self.blockers.append(res)
#             key = f"{res['name']}_{res['id']}"
#             self.commands[key] = res
#         else:
#             self.log.info("taccjm mamba env found")
# 
#     def _check_blockers(self, wait=False):
#         """ """
#         blockers = []
#         for b in self.blockers:
#             cmd_id = f"{b['name']}_{b['id']}"
#             cmd = self.get_command(cmd_id, nbytes=None)
#             if cmd["status"] == "COMPLETE":
#                 self.log.info(f"Blocker {cmd_id} completed")
#             elif cmd["status"] == "FAILED":
#                 self.log.critical(f"Blocker {cmd_id} failed: {b}")
#                 raise RuntimeError(f"Blocker {b} failed")
#             else:
#                 self.log.info(f"Blocker {cmd_id} is still running")
#                 blockers.append(cmd)
#         self.blockers = blockers
# 
#         if len(self.blockers) == 0:
#             return True
#         else:
#             return False


