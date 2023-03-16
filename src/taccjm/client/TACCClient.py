import json
import os
import pdb
import posixpath
import socket
import stat
import subprocess
import tempfile
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import List, Union

import pandas as pd

from taccjm.client import tacc_ssh_api as tsa
from taccjm.exceptions import TACCCommandError
from taccjm.utils import (check_path, filter_files, get_default_script,
                          hours_to_runtime_str, init_logger,
                          stat_all_files_and_folders, validate_file_attrs)

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
        conn_id: str = None,
        log_config: dict = None,
        ssh_config: dict = None,
    ):
        """
        Upon initializaiton:

        - Determine if running on TACC or not
        - If not running on TACC init TACC Job Manager connection
        - TODO: Tapis support
        """
        self.local_commands = {}
        self.remote_commands = {}
        self.log_config, self.log = init_logger(__name__, log_config)

        host = socket.gethostname()
        self.local = True
        if ".tacc." not in host:
            # Not running on TACC - Initialized job manager to interact with system
            self.local = False
            self.log.info("Not running on TACC. Starting SSH session.")
            self.system = (
                system
                if system is not None
                else os.environ.get("TACCJM_DEFAULT_SYSTEM")
            )
            if conn_id is None:
                if self.system is None:
                    msg = (
                        "No system detected in env variable $TACCJM_DEFAULT_"
                        + "SYSTEM and non passed"
                    )
                    self.log.critical(msg)
                    raise ValueError(msg)
                self.id = f"taccjm-{self.system}"
            else:
                self.id = conn_id

            self.ssh_client = None
            restart = False
            self.log.info(f"Looking for ssh_connection {self.id}")
            try:
                self.ssh_client = tsa.get(self.id)
                self.log.info(f"Found ssh session {self.id}", extra=self.ssh_client)
            except Exception:
                restart = True
                self.log.info(f"{self.id}, does not exist. Creating.")

            if self.ssh_client is None:
                self.log.info(f"Initializing ssh session {self.id}")
                # TODO: wrap this in try block
                self.ssh_client = tsa.init(
                    self.id,
                    self.system,
                    user=os.environ.get("CHSIM_USER"),
                    psw=os.environ.get("CHSIM_PSW"),
                    restart=restart,
                )
            self.user = self.ssh_client["user"]
            self.scratch_dir = self.ssh_client["scratch_dir"]
            self.home_dir = self.ssh_client["home_dir"]
            self.work_dir = self.ssh_client["work_dir"]
        else:
            # Running on TACC systems - Run things locally
            self.ssh_client = None
            self.system = host.split(".")[1]
            self.id = f"taccjm-{self.system}"
            self.user = self.exec("whoami")["stdout"].strip()
            self.scratch_dir = self.get_env_var("SCRATCH")
            self.home_dir = self.get_env_var("HOME")
            self.work_dir = self.get_env_var("WORK")

        # Initialize Client Directories
        self.apps_dir = posixpath.join(self.work_dir, f"{self.id}/apps")
        self.scripts_dir = posixpath.join(self.work_dir, f"{self.id}/scripts")
        self.jobs_dir = posixpath.join(self.scratch_dir, f"{self.id}/jobs")
        self.trash_dir = posixpath.join(self.scratch_dir, f"{self.id}/trash")

        # Initialize later?
        mkdir_cmnd = "mkdir -p " + " ".join(
            [
                f"{self.__getattribute__(x)}"
                for x in ["apps_dir", "scripts_dir", "jobs_dir", "trash_dir"]
            ]
        )
        self.exec(mkdir_cmnd)
        self.pm = None

    def get_env_var(self, var: str):
        """
        Resolve an environment variable
        """
        if self.local:
            return os.getenv(var)
        else:
            return tsa.exec(self.id, f"echo ${var}")["stdout"].strip()

    def abspath(self, path, force_local=False):
        """
        Makes remote path absolute if it is not. Relative paths are assumed to
        be relative to a user's scratch_dir.
        """
        if self.local or force_local:
            path = str(Path(path).resolve())
        else:
            path = (
                path
                if posixpath.isabs(path)
                else posixpath.join(self.scratch_dir, path)
            )
        return path

    def job_dir(self, job_id):
        """Get job dir given job_id"""
        return posixpath.join(self.jobs_dir, job_id)

    def job_path(self, job_id, path=None):
        """Get job dir given job_id"""
        if path is not None:
            return posixpath.join(self.jobs_dir, job_id, path)
        else:
            return posixpath.join(self.jobs_dir, job_id)

    def list_files(
        self,
        path=".",
        attrs=[
            "filename",
            "st_atime",
            "st_gid",
            "st_mode",
            "st_mtime",
            "st_size",
            "st_uid",
            "ls_str",
        ],
        recurse: bool = False,
        hidden: bool = False,
        search: str = None,
        match: str = r".",
        job_id: str = None,
        local: bool = False,
    ):
        """
        Get files and file info
        """
        if job_id is not None:
            if posixpath.isabs(path):
                raise ValueError("Path must be relative if job_id specified.")
            path = self.job_path(job_id, path)
        else:
            path = self.abspath(path, force_local=local)

        if self.ssh_client is None or local:
            return stat_all_files_and_folders(path, recurse=recurse)
        else:
            return tsa.list_files(
                self.id,
                path=path,
                attrs=attrs,
                recurse=recurse,
                hidden=hidden,
                search=search,
                match=match,
            )

    def upload(
        self,
        src_path: str,
        dest_path: str,
        job_id: str = None,
        wait: bool = True,
        file_filter="*",
        local: bool = False,
    ):
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            dest_path = self.job_path(job_id, dest_path)
        if self.ssh_client is None or local:
            fstat = self.list_files(src_path, local=local)[0]

            if stat.S_ISDIR(fstat["st_mode"]):
                src_path += "/"
            cmnd = f"rsync -a {src_path} {dest_path}"
            return self.exec(cmnd, wait=wait, local=True)
        else:
            return tsa.upload(self.id, src_path, dest_path, file_filter)

    def download(
        self,
        src_path,
        dest_path,
        job_id: str = None,
        wait: bool = True,
        file_filter: str = "*",
        local: bool = False,
    ):
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            src_path = self.job_path(job_id, src_path)
        if self.ssh_client is None or local:
            fstat = self.list_files(src_path, local=local)[0]

            if stat.S_ISDIR(fstat["st_mode"]):
                src_path += "/"
            cmnd = f"rsync -a {src_path} {dest_path}"
            return self.exec(cmnd, wait=wait, local=True)
        else:
            return tsa.download(self.id, src_path, dest_path, file_filter)

    def read(self, path, job_id: str = None, local: bool = False):
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            path = self.job_path(job_id, path)
        if self.ssh_client is None or local:
            with open(path, "r") as fp:
                if path.endswith(".json"):
                    return json.load(fp)
                else:
                    return fp.read()
        else:
            return tsa.read(self.id, path)["data"]

    def write(self, data, path: str, job_id: str = None, local: bool = False) -> None:
        """
        Wrapper to read files either locally or remotely, depending on where executing.
        """
        if job_id is not None:
            path = self.job_path(job_id, path)
        if self.ssh_client is None or local:
            if isinstance(data, str):
                with open(path, "w") as fp:
                    fp.write(data)
            else:
                with open(path, "w") as fp:
                    json.dump(data, fp)
        else:
            tsa.write(self.id, data, path)

    def _trim_cmnd_dict(self, cmnd_config, max_size=200):
        """ """
        if self.local:
            trimmed = {i: cmnd_config[i] for i in cmnd_config if i != "process"}
        else:
            trimmed = cmnd_config.copy()
        trimmed["stdout"] = trimmed["stdout"][0:max_size]
        return trimmed

    def exec(
        self,
        cmnd: str = "pwd",
        wait: bool = True,
        fail: bool = True,
        local: bool = False,
        key: str = 'SYSTEM',
    ):
        """
        Run Command

        If running on TACC, the commands will be executed using a
        python sub-process. Otherwise, command str will be written to a file
        (default bash shell) and executed remotely
        """
        cmnd_config = None
        if self.ssh_client is None or local:
            self.log.info(
                "Running command locally",
                extra={"cmnd": cmnd, "wait": wait, "error": error},
            )
            sub_proc = subprocess.Popen(
                cmnd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            cmnd_config = {
                "id": f"{len(self.local_commands.keys())}",
                "key": key,
                "cmd": cmnd,
                "ts": datetime.now(),
                "status": "STARTED",
                "stdout": "",
                "stderr": "",
                "rt": None,
                "fail": fail,
                "history": [],
                "process": sub_proc,
            }
            self.local_commands[str(cmnd_config["id"])] = cmnd_config
        else:
            cmnd_config = tsa.exec(self.id, cmnd, wait=wait,
                                   key=key, fail=fail)
            self.remote_commands[str(cmnd_config["id"])] = cmnd_config

        if wait:
            return self.process(cmnd_config["id"], wait=wait)
        else:
            return cmnd_config

    def _process_local(self, cmnd_id, wait=True):
        """
        Process a local command
        """
        self.log.info(f"Getting command {cmnd_id} from local command list")
        cmnd_config = self.local_commands[str(cmnd_id)]
        prev_status = {"status": cmnd_config["status"], "ts": cmnd_config["ts"]}
        proc = cmnd_config["process"]
        cmnd_config["rc"] = proc.poll()
        if wait or cmnd_config["rc"] is not None:
            self.log.info(f"Waiting for command {cmnd_id} to finish....")
            cmnd_config["rc"] = proc.wait()
            self.log.info(f"Command {cmnd_id} done. Reading output.")
            cmnd_config["stdout"] = proc.stdout.read().decode("utf-8")
            cmnd_config["stderr"] = proc.stderr.read().decode("utf-8")

            # update statuses
            cmnd_config["history"].append(prev_status)
            ts = datetime.now()
            cmnd_config["ts"] = ts
            cmnd_config["rt"] = (
                cmnd_config["ts"] - cmnd_config["history"][-1]["ts"]
            ).seconds
            if cmnd_config["rc"] != 0:
                cmnd_config["status"] = "FAILED"
                self.log.info(f"Command {cmnd_id} failed!")
            else:
                cmnd_config["status"] = "COMPLETE"
                self.log.info(f"Command {cmnd_id} completed!")
        else:
            self.log.info(f"Command {cmnd_id} is still running.")
            cmnd_config["stdout"] = proc.stdout.read()
            cmnd_config["status"] = "RUNNING"
            cmnd_config["ts"] = datetime.now()
            if cmnd_config["status"] != prev_status["status"]:
                cmnd_config["history"].append(prev_status)

        return cmnd_config

    def process(self,
                cmnd_id=None,
                poll=True,
                wait=True,
                error=True,
                nbytes=None,
                local: bool = False):
        """
        Poll an executed command to see if it has completed.

        TODO: implement cmnd_id = None -> Process all
        """
        if self.ssh_client is None or local:
            if cmnd_id is None:
                self.log.info("Getting active all active local commands")
                active = [c for c in self.local_comands.values()
                          if c['status'] == 'RUNNING']
            else:
                active = [self.local_commands[str(cmnd_id)]]

            if not poll:
                return active

            processed_commands = [self._process_local(c, wait)
                                  for c in active]
        else:
            processed_commands = tsa.process(
                    self.id, cmnd_id=cmnd_id,
                    nbytes=nbytes, wait=wait,
                    poll=poll)

            for cmnd_config in processed_commands:
                self.remote_commands[str(cmnd_config["id"])] = cmnd_config

            if not poll:
                return processed_commands

        failed = []
        for cmnd_config in processed_commands:
            logconfig = {i: cmnd_config[i] for i in cmnd_config
                         if i != "process"}
            if cmnd_config["status"] == "FAILED":
                msg = f"Command {cmnd_id} failed"
                self.log.error(msg, extra={"cmnd_config": logconfig})
                if cmnd_config['fail']:
                    failed.append(cmnd_config)

            self.log.info(
                f"Processed command {cmnd_id}",
                extra={"cmnd_config": self._trim_cmnd_dict(cmnd_config)},
            )

        if error and len(failed) > 0:
            raise TACCCommandError(self.system, self.user, failed)

        return processed_commands

    def rm(self, remote_path: str, restore=False) -> None:
        """
        Removes/Restores a file/folder from the trash directory by moving
        it to/from its original path.

        TODO: Implement compression

        Parameters
        ----------
        remote_path : str
            Unix-style path of the file/folder that should not exist anymore to
            restore.
        restore : bool, default=False`
            If path has been removed already, use restore=True to restore it.

        Returns
        -------
        TODO: Document

        Raises
        ------
        FileNotFoundError
            If the file to be restored does not exist in trash directory.
        TJMCommandError
            If error moving data from trash to its original path. This could
            be becasue the original path still has data in it/exists.
        """
        # Unix paths -> Get file remote file name and directory
        remote_path = self.abspath(remote_path)
        file_name = remote_path.replace("/", "___")
        trash_path = f"{self.trash_dir}/{file_name}"

        if not restore:
            src_path = remote_path
            dest_path = trash_path
        else:
            src_path = trash_path
            dest_path = remote_path

        fstat = self.list_files(src_path)[0]
        common = posixpath.commonpath([self.trash_dir, fstat["filename"]])
        if common.startswith(self.trash_dir) and not restore:
            msg = "Trying to remove something already inside of the trash dir"
            self.log.error(msg, extra={"remote_path": remote_path, "restore": restore})
            raise ValueError(msg)

        if stat.S_ISDIR(fstat["st_mode"]):
            src_path += "/"
        cmnd = f"rsync -a {src_path} {dest_path} && rm -rf {src_path}"

        ret = self.exec(cmnd, wait=False)

        return ret

    def ls_trash(
        self,
        attrs=[
            "filename",
            "st_atime",
            "st_gid",
            "st_mode",
            "st_mtime",
            "st_size",
            "st_uid",
            "ls_str",
        ],
    ):
        """
        List trash contents
        """
        trash_contents = self.list_files(self.trash_dir, attrs=attrs, recurse=True)
        for t in trash_contents:
            t["filename"] = posixpath.basename(t["filename"]).replace("___", "/")

        return trash_contents

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
        if not check_path(filter_str):
            raise ValueError(f"Invalid vilter string {filter_str}")

        rem_path = posixpath.join(self.trash_dir, filter_str)
        rem_path = posixpath.abspath(rem_path)

        cmnd = f"rm -rf {self.trash_dir}/{filter_str}"
        ret = self.exec(cmnd, wait=False)

        return ret

    def deploy_script(
        self, script_name: str, local_file: str = None, script_str: str = None
    ) -> None:
        """
        Deploy a script to TACC

        Parameters
        ----------
        script_name : str
            The name of the script. Will be used as the local filename unless
            local_file is passed. If the filename ends in .py, it will be
            assumed to be a Python3 script. Otherwise, it will be treated as a
            generic executable.
        local_file : str, optional
            The local filename of the script if not passed, will be inferred
            from script_name.
        script_str : str, optional
            Can also pass a string directly that will be saved to a bash script
            with the given script_name.
        Returns
        -------
        """
        local_fname = local_file if local_file is not None else script_name
        if not os.path.exists(local_fname) and script_str is None:
            raise ValueError(f"Could not find script file - {local_fname} " +
                             "and no script string specified!")

        # Extract basename in case the script_name is a path
        script_name, ext = os.path.splitext(os.path.basename(script_name))
        remote_path = posixpath.join(self.scripts_dir, script_name)

        # If python file, add directive to python3 path on TACC system
        if script_str is None:
            if ext == ".py":
                script = "#!" + self.python_path + "\n"
                with open(local_fname, "r") as fp:
                    script += fp.read()
                self.write(script, remote_path)
            else:
                self.upload(local_fname, remote_path)
        else:
            if ext == ".py":
                script = "#!" + self.python_path + "\n" + script_str
            else:
                script = "#!/bin/bash\n" + script_str
            self.write(script, remote_path)

        # Make remote script executable
        self.exec(f"chmod +x {remote_path}")

    def run_script(
        self,
        script_name: str,
        job_id: str = None,
        args: List[str] = [],
        logfile: bool = False,
        errfile: bool = True,
    ) -> str:
        """
        Run a pre-deployed script on TACC.

        Parameters
        ----------
        script_name : str
            The name of the script, without file extensions.
        job_id : str
            Job Id of job to run the script on.  If passed, the job
            directory will be passed as the first argument to script.
        args : list of str
            Extra commandline arguments to pass to the script.

        Returns
        -------
        out : str
            The standard output of the script.
        """
        if job_id is not None:
            args.insert(0, "/".join([self.jobs_dir, job_id]))

        ts = datetime.fromtimestamp(time.time()).strftime("%Y%m%d_%H%M%S")
        run_cmd = f"{self.scripts_dir}/{script_name} {' '.join(args)}"
        if logfile:
            logfile = f"{self.scripts_dir}/{script_name}_{ts}_log"
            run_cmd += f" > {logfile}.txt"
        else:
            logfile = ""
        if errfile:
            errfile = f"{self.scripts_dir}/{script_name}_{ts}_err"
            run_cmd += f" 2> {errfile}.txt"
            errfile = ""

        cmnd_config = self.exec(run_cmd, wait=False)

        return cmnd_config

    def list_scripts(self) -> List[str]:
        """
        List scripts deployed in this TACCJobManager Instance

        Parameters
        ----------

        Returns
        -------
        scripts : list of str
            List of scripts in TACC Job Manager's scripts directory.
        """
        sc = [
            f["filename"] for f in self.list_files(path=self.scripts_dir, recurse=True)
        ]
        return sc

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
            path=self.jobs_dir,
            attrs=["filename", "st_mode"],
            hidden=False,
            recurse=True,
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
        job_config_path = self.job_path(job_id, "job.json")
        try:
            job_config = self.read(job_config_path)
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
        cmnd = f"cd {job_config['job_dir']} && "
        cmnd += f"sbatch {job_config['submit_script']}"
        ret = self.exec(cmnd, fail=True)
        job_config["slurm_id"] = ret["stdout"].split("\n")[-2].split(" ")[-1]

        # Save job config
        self.write(job_config, self.job_path(job_id, "job.json"))

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
                self.exec(cmnd)
            except TACCCommandError as c:
                self.log.error(
                    f"cancel_job - Failed to cancel job {job_id}.",
                    extra={"cmnd_config": c.command_config},
                )
                raise c

            # Remove slurm ID and store into job hist
            job_config["slurm_hist"] = job_config.get("slurm_hist", [])
            job_config["slurm_hist"].append(job_config.pop("slurm_id"))

            # Save updated job config
            self.write(job_config, self.job_path(job_id, "job.json"))
        else:
            msg = f"Job {job_id} has not been submitted yet."
            self.log.error(msg)
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

        rem_res = self.rm(job_dir)

        return rem_res

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

        rm_cmnd = self.rm(job_dir, restore=True)
        try:
            self.process(rm_cmnd["id"], wait=True)
        except TACCCommandError as t:
            msg = f"restore_job - Job {job_id} restore command failed."
            self.log.error(msg, extra={"command_config": t.command_config})
            raise t

        # Return restored job config
        job_config = self.get_job(job_id)

        return job_config

    def _mamba_install(
        self,
        exe="Mambaforge-pypy3-Linux-x86_64.sh",
        url="https://github.com/conda-forge/miniforge/" + "releases/latest/download",
    ):
        """
        Install mamba on TACC system
        """
        exe_path = posixpath.join(self.work_dir, exe)
        install_path = posixpath.join(self.work_dir, "mambaforge")
        mamba_path = posixpath.join(install_path, "mambafore/bin/mamba")
        conda_path = posixpath.join(install_path, "mambafore/bin/conda")
        install_cmd = f"rm -rf {exe_path} && "
        install_cmd += f"wget -P {self.work_dir} {url}/{exe} && "
        install_cmd += f"chmod +x {exe_path} && "
        install_cmd += f"{exe_path} -b -p $WORK/mambaforge && "
        install_cmd += f"rm {exe_path} && "
        install_cmd += f"{conda_path} init && "
        install_cmd += f"{mamba_path} init"

        pdb.set_trace()
        install_cmd = self.exec(install_cmd)

        return install_cmd

    def _find_pm(self):
        """
        Find package manager in environment looks for mamba first. If can't
        find looks for conda.
        """
        mamba_res = self.exec("mamba --help", wait=False)
        conda_res = self.exec("conda --help", wait=False)
        mamba_res = self.process(mamba_res["id"], wait=True)
        if mamba_res["rc"] != 0:
            conda_res = self.process(conda_res["id"], wait=True)
            if conda_res["rc"] == 0:
                self.pm = "conda"
        else:
            self.pm = "mamba"

        if self.pm is None:
            self.log.info(
                "No mamba/conda env found. Installing",
                extra={"mamba_res": mamba_res, "conda_res": conda_res},
            )
            self._mamba_install()
            self.log.info("Mamba install complete")
            self.pm = "mamba"

    def get_python_env(self, env: str = None):
        """
        Returns python environments installed, or packages
        """
        if self.pm is None:
            self._find_pm()

        if env is None:
            cmnd = f"{self.pm} env list"
            cols = ["name", "path"]
            skiprows = 2
        else:
            cmnd = f"{self.pm} list --name {env}"
            cols = ["name", "version", "build", "channel"]
            skiprows = 3
        res = self.exec(cmnd, wait=True, fail=True)
        fp = StringIO(res["stdout"].replace("*", " "))

        if env is not None:
            first_line = fp.readline()
            env_path = first_line.strip().split(" ")[-1][:-1]
            skiprows -= 1

        df = pd.read_csv(
            fp, delim_whitespace=True, names=cols, header=None, skiprows=skiprows
        )

        if env is not None:
            df["path"] = env_path

        return df

    def get_install_env(self, env: str, conda: str = None, pip: str = None):
        """
        Conda install

        This should only be run from non-TACC systems to verify setup is correct on
        TACC systems. Installs conda (mamba) and setups ch-sim conda environment as
        necessary.
        """
        if self.pm is None:
            self._find_pm()

        self.log.info(
            f"Getting/Installing {env}",
            extra={"pm": self.pm, "env": env, "conda": conda, "pip": pip},
        )

        envs = self.get_python_env()
        if not any(envs["name"] == env):
            # Now create environment using package manager found
            self.log.info(f"{env} does not exist. Creating now.")
            self.exec(f"{self.pm} create -y --name {env}")

        if conda is not None:
            self.log.info("Installing conda packages")
            self.exec(f"{self.pm} install -n {env} -y {conda}")

        if pip is not None:
            envs = self.get_python_env()
            env_path = envs["path"][envs["name"] == env].iloc[0]
            pip_path = f"{env_path}/bin/pip"
            self.log.info(f"Installing pip packages using pip path {pip_path}")
            self.exec(f"{pip_path} install {pip}")

        python_env = self.get_python_env(env)
        self.log.info(
            "Environment configuration done", extra={"env": python_env.to_dict()}
        )

        return python_env

    def showq(self, user: str = None) -> List[dict]:
        """
        Get information about jobs currently in the job queue.

        Parameters
        ----------
        user : string, default=None
            User to get queue info for user. If none, then defaults to user
            connected to system. Pass `all` to get system for all users.

        Returns
        -------
        jobs: list of dict
            List of dictionaries containing inf on jobs in queue including:
                - job_id      : ID of job in queue
                - job_name    : Name given to job
                - username    : User who submitted to job
                - state       : Job queue state
                - nodes       : Number of nodes job requires.
                - remaining   : Remaining time left for job to execute.
                - start_time  : Time job started exectuing.

        Raises
        ------
        TJMCommandError
            If slurm queue is not accessible for some reason (TACCC error).
        """
        # Build command string
        cmnd = "showq"
        slurm_user = self.user if user is None else user
        if slurm_user != "all":
            cmnd += f" -U {slurm_user}"

        # Query job queue
        try:
            ret = self.exec(cmnd)
        except TACCCommandError as t:
            msg = "showq - TACC SLURM queue is not accessible."
            self.log.error(msg)
            raise t
        ret = ret["stdout"]

        # Loop through lines in output table and parse job information
        jobs = []
        lines = ret.split("\n")
        jobs_line = False
        line_counter = -2
        for line in lines:
            job_statuses = ["ACTIVE", "WAITING", "BLOCKED", "COMPLETING"]
            if any([line.startswith(x) for x in job_statuses]):
                jobs_line = True
                continue
            if jobs_line:
                if line == "":
                    jobs_line = False
                    line_counter = -2
                    continue
                else:
                    line_counter += 1
                    if line_counter > 0:
                        split_line = line.split()
                        jobs.append(
                            {
                                "job_id": split_line[0],
                                "job_name": split_line[1],
                                "username": split_line[2],
                                "state": split_line[3],
                                "nodes": split_line[4],
                                "remaining": split_line[4],
                                "start_time": split_line[5],
                            }
                        )

        return jobs

    def get_allocations(self) -> List[dict]:
        """
        Get information about users current allocations.

        Parameters
        ----------

        Returns
        -------
        allocations: list of dict
            List of dictionaries containing allocation info including:
                - name
                - service_units
                - exp_date

        Raises
        ------
        TJMCommandError
            If allocations file is not accessible for some reason (TACCC error).
        """
        # Check job allocations
        cmnd = "/usr/local/etc/taccinfo"
        try:
            ret = self.exec(cmnd)
        except TACCCommandError as t:
            msg = "get_allocations - Unable to get allocation info"
            self.log.error(msg)
            raise t
        ret = ret["stdout"]

        # Parse allocation info
        allocations = set([x.strip() for x in ret.split("\n")[2].split("|")])
        allocations.remove("")
        allocations = [x.split() for x in allocations]
        allocations = [
            {"name": x[0], "service_units": int(x[1]), "exp_date": x[2]}
            for x in allocations
        ]

        return allocations


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
