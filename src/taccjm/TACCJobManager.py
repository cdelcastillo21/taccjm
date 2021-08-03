"""
TACCJobManager Class
Carlos del-Castillo-Negrete
cdelcastillo21@gmail.com

File containing implementation of TACCJobManager class and supporting functions.

References:

"""


import os                       # OS system utility functions
import errno                    # For error messages
import tarfile                  # For sending compressed directories
import fnmatch                  # For unix-style filename pattern matching
import re                       # Regular Expressions
import pdb                      # Debug
import json                     # For saving and loading job configs to disk
import time                     # Time functions
import logging                  # Used to setup the Paramiko log file
import datetime                 # Date time functionality
import configparser             # For reading configs
from jinja2 import Template     # For templating input json files

from taccjm.SSHClient2FA import SSHClient2FA  # Modified paramiko client
from paramiko import SSHException, AuthenticationException, BadHostKeyException


logger = logging.getLogger(__name__)


class TJMCommandError(Exception):
    """
    Custom exception to wrap around any failed commands executed on a TACC resource.
    This exception gets thrown first by the _execute_command method of the
    TACCJobManager class, upon any command executed that returns a non-zero return code.
    The idea is to handle this exception gracefully and throw a more specific error
    in other methods, or pass exception along up the stack with a better message/context
    as to where it was thrown to diagnosis the core issue.

    Attributes
    ----------
    system : str
        TACC System on which command was executed.
    user : str
        User that is executing command.
    command : str
        Command that threw the error.
    rc : str
        Return code.
    stdout : str
        Output from stdout.
    stderr : str
        Output from stderr.
    message : str
        Explanation of the error.
    """

    def __init__(self, system, user, command, rc, stderr, stdout,
            message="Non-zero return code."):
        self.system = system
        self.user = user
        self.command = command
        self.rc = rc
        self.stderr = stderr.strip('\n')
        self.stdout = stdout.strip('\n')
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        msg =  f"\n{self.message}"
        msg += f"\n{self.user}@{self.system}$ {self.command}"
        msg += f"\nrc     : {self.rc}"
        msg += f"\nstdout : {self.stdout}"
        msg += f"\nstderr : {self.stderr}"
        return msg


class TJMCommandError(Exception):
    """
    Custom exception to wrap around executions of any commands sent to TACC resource.
    This exception gets thrown first by the _execute_command method of the
    TACCJobManager class, upon any command executing returning a non-zero return code.
    The idea is to handle this exception gracefully and throw a more specific error
    in other methods, with the last case being just being passing thus exception along
    up the stack with context as to where it was thrown to diagnosis the core issue.

    Attributes
    ----------
    system : str
        TACC System on which command was executed.
    user : str
        User that is executing command.
    command : str
        Command that threw the error.
    rc : str
        Return code.
    stdout : str
        Output from stdout.
    stderr : str
        Output from stderr.
    message : str
        Explanation of the error.
    """

    def __init__(self, system, user, command, rc, stderr, stdout,
            message="Non-zero return code."):
        self.system = system
        self.user = user
        self.command = command
        self.rc = rc
        self.stderr = stderr.strip('\n')
        self.stdout = stdout.strip('\n')
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        msg =  f"\n{self.message}"
        msg += f"\n{self.user}@{self.system}$ {self.command}"
        msg += f"\nrc     : {self.rc}"
        msg += f"\nstdout : {self.stdout}"
        msg += f"\nstderr : {self.stderr}"
        return msg


class TACCJobManager():
    """
    Class defining an ssh connection to a TACC resource.

    Attributes
    ----------
    system : str
        Name of tacc system to connect to. Supported systems:
        stampede2, ls5, frontera, maverick2
    user : str
        Name of tacc user connecting via ssh to resource.
    jobs_dir : str
        Directory where jobs for this job manager instance can be found.
    apps_dir : str
        Directory where applications for this job manager instance can be found.

    Methods
    -------

    """

    TACC_SYSTEMS = ['stampede2', 'ls5', 'frontera', 'maverick2']
    TACC_USER_PROMPT = "Username:"
    TACC_PSW_PROMPT = "Password:"
    TACC_MFA_PROMPT ="TACC Token Code:"
    TACCJM_DIR = "$SCRATCH"
    SUBMIT_SCRIPT_TEMPLATE = """#!/bin/bash
#----------------------------------------------------
# {job_name}
# {job_desc}
#----------------------------------------------------

#SBATCH -J {job_id}                               # Job name
#SBATCH -o {job_id}.o%j                           # Name of stdout output file
#SBATCH -e {job_id}.e%j                           # Name of stderr error file
#SBATCH -p {queue}                                # Queue (partition) name
#SBATCH -N {N}                                    # Total # of nodes
#SBATCH -n {n}                                    # Total # of mpi tasks
#SBATCH -t {rt}                                   # Run time (hh:mm:ss)"""


    def __init__(self, system, user=None, psw=None, mfa=None, apps_dir='taccjm-apps',
            jobs_dir='taccjm-jobs'):
        """
        Parameters
        ----------
        system : str
            Name of tacc system to connect to via ssh. Supported systems:
            stampede2, ls5, frontera, maverick2
        user : str , optional
            Name of tacc user connecting. If non provided input prompt will appear
        psw : str , optional
            Password for user connecting. If non provided input prompt will appear
        mfa : str , optional
            2-Factor Authenticaion token for user.. If non provided input prompt will appear
        apps_dir : str , optional
            Directory relative to TACCJM_DIR directory to place apps managed by this taccjm instance
            (default is taccjm-apps)
        jobs_dir : str , optional
            Directory relative to TACCJM_DIR directory to place jobs managed by this taccjm instance
            (default is taccjm-jobs)
        """

        if system not in self.TACC_SYSTEMS:
            msg = f"Unrecognized TACC system {system}. Must be one of {self.TACC_SYSTEMS}."
            logger.error(msg)
            raise ValueError(msg)

        self.system= f"{system}.tacc.utexas.edu"

        # Connect to server
        logger.info(f"Connecting {user} to TACC system {system}...")
        self._client = SSHClient2FA(user_prompt=self.TACC_USER_PROMPT,
                psw_prompt=self.TACC_PSW_PROMPT,
                mfa_prompt=self.TACC_MFA_PROMPT)
        self._client.load_system_host_keys()
        self.user = self._client.connect(self.system, uid=user, pswd=psw, mfa_pswd=mfa)
        logger.info(f"Succesfuly connected to {system}")

        # Set and Create jobs and apps dirs if necessary
        # TODO: Catch and handle TJMCommandErrors for common things
        taccjm_path = self._execute_command(f"echo {self.TACCJM_DIR}").strip()
        self.jobs_dir = '/'.join([taccjm_path, jobs_dir])
        self.apps_dir = '/'.join([taccjm_path, apps_dir])

        logger.info("Creating apps/jobs dirs if they don't already exist")
        self._execute_command(f"mkdir -p {self.jobs_dir}")
        self._execute_command(f"mkdir -p {self.apps_dir}")


    def _load_project_config(self, ini_file):
        """
        Loads a local .ini file at a specified path.

        Parameters
        ----------
        ini_file : str
            Local path to .ini file.

        Returns
        -------
        config : dict
            json config from file templated appropriately.

        Raises
        ------
        FileNotFoundError
            if json file does not exist
        """
        # Check if it exists - If it doesn't config parser won't error
        if not os.path.exists(project_config_file):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), project_config_file)

        # Read project config file
        config_parse = configparser.ConfigParser()
        config_parse.read(project_config_file)
        config = config_parse._sections

        # Return as dictionary
        return config


    def _load_templated_json_file(self, path, config):
        """
        Loads a local json config function at path and templates it
        using jinja with the values found in config. For example, if
        json file contains `{{ a.b }}`, and `config={'a':{'b':1}}`,
        then `1` would be substituted in (note nesting).

        Parameters
        ----------
        path : str
            Local path to json file.
        config : dict
            Dictionary with values to substitute in for jinja templates
            in json file.

        Returns
        -------
        config : dict
            json config from file templated appropriately.

        Raises
        ------
        FileNotFoundError
            if json file does not exist

        """
        with open(path) as file_:
            config = json.loads(Template(file_.read()).render(config))
            return config


    def _execute_command(self, cmnd):
        """
        Executes a shell command through ssh on a TACC resource.

        Parameters
        ----------
        cmnd : str
            Command to execute. Be careful! rm commands and such will delete things permenantly!

        Returns
        -------
        out : str
            stdout return from command.

        Raises
        ------
        TJMCommandError
            If command executed on TACC resource returns a non-zero return code .
        """
        try:
            stdin, stdout, stderr = self._client.exec_command(cmnd)
        except SSHException as ssh_eror:
            # Will only occur if ssh connection is broken
            mgs = "Unalbe to excecute command. TACCJM ssh connection error: {ssh_err.__str__()}"
            logger.error(msg)
            raise ssh_error

        out = stdout.read().decode('utf-8')
        err = stderr.read().decode('utf-8')
        rc = stdout.channel.recv_exit_status()

        if rc!=0:
            # Build base TJMCommand Error, only place this should be done
            t = TJMCommandError(self.system, self.user, cmnd, rc, out, err)

            # Only log the actual TJMCommandError object once, here
            logger.error(t.__str__)

            raise t

        return out


    def showq(self, user=None, prnt=False):
        """
        Get information about jobs currently in the job queue.

        Parameters
        ----------
        user : string, default=None
            User to get queue info for user. If none, then defaults to user
            connected to system. Pass `all` to get system for all users.

        Returns
        -------
        jobs: dict
            Dictionary containing job info - job_id, job_name,
            username, state, nodes, remaining, start_time

        Raises
        ------
        TJMCommandError
            If slurm queue is not accessible for some reason (TACCC system error).
        """
        # Build command string
        cmnd = 'showq '
        slurm_user = self.user if user is None else user
        if slurm_user!='all':
            cmnd += f"-u {user}"

        # Query job queue
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "showq - TACC SLURM queue is not accessible."
            logger.error(tjm_error.message))
            raise tjm_error

        # Parse squeue output - look for indices of header lines in table
        lines = ret.split('\n')
        idxs = [i for i, x in enumerate(['ACTIVE' in x or 'WAITING' in x
            or 'COMPLETING' in x for x in lines]) if x]

        # Get active, waiting, and completed/errored job sections of table
        jobs.append([x.split() for x in lines[idxs[0]+3:idxs[1]-1]])
        jobs.append([x.split for x in lines[idxs[1]+3:idxs[2]-1]])
        if len(idxs)>2:
            jobs.append([x.split() for x in lines[idxs[2]+3:len(lines)-3]])
        jobs = [{'job_id':x[0], 'job_name':x[1],
                 'username':x[2], 'state':x[3],
                 'nodes':x[4], 'remaining': x[4],
                 'start_time': x[5]} for x in jobs]

        return jobs


    def get_allocations(self):
        """
        Get information about users current allocations.

        Parameters
        ----------

        Returns
        -------
        allocations: dict
            Dictionary containing allocation info - name, service_units, exp_date

        Raises
        ------
        TJMCommandError
            If slurm queue is not accessible for some reason (TACCC system error).
        """
        # Check job allocations
        cmd = '/usr/local/etc/taccinfo'
        ret = self._execute_command(cmd)
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "get_allocations - Unable to get allocation info"
            logger.error(tjm_error.message)
            raise tjm_error

        # Parse allocation info
        allocations = set([x.strip() for x in ret.split('\n')[2].split('|')])
        allocations.remove('')
        allocations = [x.split() for x in allocations]
        allocations = [{'name':x[0],
                        'service_units': int(x[1]),
                        'exp_date': x[2]} for x in allocations]

        return allocations


    def list_files(self, path='~'):
        """
        Returns the value of `ls -lat <path>` command TACC system. Paths are
        relative to user's home directory and use unix path seperator '/'
        since all TACC systems are unix.

        Parameters
        ----------
        path : str, optional, default = ~
            Path, relative to user's home directory, to perform `ls -lat` on.

        Returns
        -------
        files : list of str
            List of files in directory, sorted by last modified timestamp.

        Raises
        ------
        TJMCommandError
            If can't access path for any reason on TACC system. This may be
            because the TACC user doesn't have permissions to view the given
            directory or that the path does not exist, for exmaple.

        """
        cmnd = f"ls -lat {path}"

        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = f"list_files - unable to access {path}"
            logger.error(tjm_error.message)
            raise tjm_error

        # Return list of files
        files = [re.split("\\s+", x)[-1] for x in re.split("\n", ret)[1:]]
        for v in ['', '.', '..', None]:
            if v in files:
                files.remove(v)

        # Sort and return file list
        files.sort()
        return files


    def peak_file(self, path, head=-1, tail=-1):
        """
        Performs head/tail on file at given path to "peak" at file.

        Parameters
        ----------
        path : str
            Unix-style path, relative to users home dir, of file to peak at.
        head : int, optional, default=-1
            If greater than 0, then read exactly `head` many lines from the top `path`.
        tail : int, optional, default=-1
            If greater than 0, then read exactly `tail` many lines from the bottom `path`.
            Note: if head is specified, then tail is ignored.

        Returns
        -------
        txt : str
            Raw text from file.

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.
        PermissionError
            If user does not have permission to write to specified remote path
            on TACC system or access to local file/folder and contents.

        Warnings
        --------
        Will overwrite existing files and folders and is recursive for folders being sent.
        Remote paths must use unix path seperator '/' since all TACC systems are unix.

        """
        if head>0:
            cmnd = f"head -{head} {path}"
        elif tail>0:
            cmnd = f"tail -{tail} {path}"
        else:
            cmnd = f"head {path}"
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandErrror as t:
            if 'Permission denied' in t.stderr:
                msg = f"peak_file - Dont have permission to access {path}"
                raise PermissionError(errno.EACCES, msg, path)
            elif 'No such file or directory' in t.stderr:
                msg = f"peak_file - No such file or directory {path}"
                logger.error(msg)
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
            else:
                t.message = f"peak_file - Unexpected error."
                logger.error(t.message)
                raise t

        return ret


    def send_data(self, local, remote, file_filter='*'):
        """
        Sends file or folder from local path to remote path. If a file is
        specified, the remote path is the destination path of the file to be sent
        If a folder is specified, all folder contents (recursive) are compressed
        into a .tar.gz file before being sent and then the contents are unpacked
        in the specified remote path.

        Parameters
        ----------
        local : str
            Path to local file or folder to send to TACC system.
        remote : str
            Destination unix-style path for the file/folder being sent on the TACC system.
            If a file is being sent, remote is the destination path. If a folder is
            being sent, remote is the folder where the file contents will go.
            Note that if path not absolute, then it's relative to user's home directory.
        file_filter: str, optional, Default = '*'
            If a folder is being download, unix style pattern matching string to use on
            files to download. For example, '*.txt' would only download .txt files.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.
        PermissionError
            If user does not have permission to write to specified remote path
            on TACC system or access to local file/folder and contents.
        TJMCommandError
            If a directory is being sent, this error is thrown if there are any
            issues unpacking the sent .tar.gz file in the destination directory.

        Warnings
        --------
        Will overwrite existing files and folders and is recursive for folders being sent.
        Remote paths must use unix path seperator '/' since all TACC systems are unix.

        """
        # Unix paths -> Get file remote file name and directory
        remote_fname = '/'.join(remote.strip('/').split('/')[-1:])
        remote_dir = '/'.join(remote.strip('/').split('/')[:-1])
        remote_dir = '.' if remote_dir=='' else remote_dir

        try:
            # Sending directory -> Package into tar file
            if os.path.isdir(local):
                fname = os.path.basename(local)
                local_tar_file = f".{fname}.taccjm.tar"
                remote_tar_file = f"{remote_dir}/.taccjm_temp_{fname}.tar"

                # Package tar file -> Recursive call
                with tarfile.open(local_tar_file, "w:gz") as tar:
                    f = lambda x : x if fnmatch.fnmatch(x, file_filter) else None
                    tar.add(local, arcname=remote_fname, filter=f)

                # Send tar file
                try:
                    with self._client.open_sftp() as sftp:
                        res = sftp.put(local_tar_file, remote_tar_file)
                except Exception as e:
                    # Remove local tar file before passing on exception
                    os.remove(local_tar_file)
                    raise e

                # Remove local tar file that was just sent successfully
                os.remove(local_tar_file)

                # Now untar file in destination and remove remote tar file
                # If tar command fails, the remove command should work regardless
                untar_cmd = f"tar -xzvf {remote_tar_file} -C {remote_dir}; rm -rf {remote_tar_file}"
                _ = self._execute_command(untar_cmd)
            # Sending file
            elif os.path.isfile(local):
                with self._client.open_sftp() as sftp:
                    res = sftp.put(local, remote)
            else:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), local)
        except FileNotFoundError as f:
            msg = f"send_file - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"send_file - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except TJMCommandErrror as t:
            t.message = f"send_file - Error unpacking tar file in remote directory"
            logger.error(t.message)
            raise t
        except Exception as e:
            msg = f"send_file - Unexepcted error {e.__str__()}"
            logger.error(msg)
            raise e


    def get_data(self, remote, local, file_filter='*'):
        """
        Downloads file or folder from remote path on TACC resoirce to local path. If a
        file is specified, the local path is the destination path of the file to be
        downloaded If a folder is specified, all folder contents (recursive) are
        compressed into a .tar.gz file before being downloaded and the contents are
        unpacked in the specified local directory.

        Parameters
        ----------
        remote : str
            Unix-style path to file or folder on TACC system to download.
            Note that if path is not absolute, then it's relative to user's home directory.
        local : str
            Destination where file/folder being downloaded will be placed.
            If a file is being downloaded, then local is the destination path.
            If a folder is being downloaded, local is the folder where the contents will go.
        file_filter: str, optional, default='*'
            If a folder is being download, unix style pattern matching string to use on
            files to download. For example, '*.txt' would only download .txt files.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If local or remote file/folder do not exist
        PermissionError
            If user does not have permission to write to specified remote path
            on TACC system or access to local file/folder and contents.
        TJMCommandError
            If a directory is being downloaded, this error is thrown if there are any
            issues packing the .tar.gz file on the remote system before downloading.

        Warnings
        --------
        Will overwrite existing files and folders and is recursive for folders being downloaded.
        Remote paths must use unix path seperator '/' since all TACC systems are unix.

        """

        is_dir = True
        local = local.rstrip('/')
        try:
            # Below command is a way of checking if file is a directory, if it is,
            # It will proceed with tar-ing the file. If not we should get an error
            fname = os.path.basename(remote)
            cmd = f"cd {remote}" + "/.. && { tar -czvf " + f"{fname}.tar.gz {fname}" +"; }"
            self._execute_command(cmd)

            # Transfering tar file instead
            local_dir = os.path.abspath(os.path.join(local, os.pardir))
            local = f"{local_dir}/{fname}.tar.gz"
            remote = f"{remote}.tar.gz"
        except TJMCommandErrror as t:
            if 'Not a directory' in t.stderr:
                # Could still be a file, continue
                is_dir = False
                pass
            elif 'Permission denied' in t.stderr:
                msg = f"get_data - Permission denied on {remote}"
                logger.error(msg)
                raise PermissionError(errno.EACCES, msg, remote)
            else:
                t.message = f"get_data - Error compressing data to download."
                logger.error(t.message)
                raise t

        # Transfer the data
        try:
            with self._client.open_sftp() as sftp:
                sftp.get(local, remote)
        except FileNotFoundError as f:
            msg = f"get_data - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"get_data - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except Exception as e:
            msg = f"get_data - Unknown error downloading data via sftp."
            logger.error(t.message)
            raise e

        if is_dir:
            with tarfile.open(local) as tar:
                tar.extractall(path=local)
            os.remove(local)


    # TODO: Implement sending pickled data?
    def send_stream(self, data, path):
        """
        Send `data` directly to path via an sftp file stream. Supported data types are:
            1. dict -> json file
            2. text -> text file

        Parameters
        ----------
        data : dict or str
            Dictionary of data to send and save as a json file if dict or text
            data to be saved as text file if str.
        path : str
            Unix-style path on TACC system to save data to.

        Returns
        -------
        None

        """
        d_type = type(data)
        if d_type not in [dict, str]
            raise ValueError(f"Data type {d_type} is not supported")
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(path, 'w') as jc:
                    if d_type==dict:
                        json.dump(data, jc)
                    else:
                        jc.write(data)
        # TODO: Handle other exceptions?
        except Exception as e:
            msg = f"Unable to write {d_type} data to {path}"
            logger.error(msg)
            raise e


    # TODO: Implement getting pickled data?
    def get_stream(self, path, data_type='text'):
        """
        Get data of `data_type` in file `path` on remote TACC system directly
        via a file stream. Supported data types are:
            1. text -> str (Default)
            2. json -> dict

        Parameters
        ----------
        path : str
            Unix-style path on TACC system containing desired data.
        data_type : str, optional, Default = 'str'
            Type of data to get from desired file. Currently only 'text'
            and 'json' data types are supported.

        Returns
        -------
        None

        """
        d_type = type(data)
        if d_type not in [dict, str]
            raise ValueError(f"Data type {d_type} is not supported")
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(path, 'w') as jc:
                    if d_type==dict:
                        json.dump(data, jc)
                    else:
                        jc.write(data)
        # TODO: Handle other exceptions?
        except Exception as e:
            msg = f"Unable to read {data_type} data from {path}."
            logger.error(msg)
            raise e



    def get_apps(self):
        apps = self.list_files(path=self.apps_dir)

        return apps


    def get_app(self, app_id):

        # Get current apps already deployed
        cur_apps = self.get_apps()
        if appId not in cur_apps:
            msg = f"Application {appId} does not exist."
            logger.error(msg)
            raise Exception(msg)

        # Load application config
        app_config_path = '/'.join([self.apps_dir, appId, 'app.json'])
        app_config = self.get_json(app_config_path)

        # Get wrapper script
        try:
            wrapper_script = '/'.join([self.apps_dir, appId, app_config['templatePath']])
            cmnd = f"cat {wrapper_script}"
            app_config['wrapper_script'] = self._execute_command(cmnd)
        except Exception as e:
            msg = "App main entry point not found for app {appId}"
            logger.error(msg)
            raise FileNotFoundError

        return app_config


    def deploy_app(self, local_app_dir='.', app_config_file="app.json",
            proj_config_file="project.ini", overwrite=False):

        # Load project configuration file
        proj_config_path = os.path.join(local_app_dir, proj_config_file)
        proj_config = self.load_project_config(proj_config_path)

        # Load templated app configuration
        app_config_path = os.path.join(local_app_dir, app_config_file)
        app_config = self.load_templated_json_file(app_config_path,  proj_config)

        # Get current apps already deployed
        cur_apps = self.get_apps()

        # Required parameters for application configuration
        required_args = ['name', 'shortDescription', 'defaultQueue', 'defaultNodeCount',
                'defaultProcessorsPerNode', 'defaultMaxRunTime', 'templatePath', 'inputs',
                'parameters', 'outputs']
        missing = set(required_args) - set(app_config.keys())
        if len(missing)>0:
            msg = f"Missing required app configs {missing}"
            logger.error(msg)
            raise Exception(msg)

        # Only overwrite previous version of app (a new revision) if overwrite is set.
        if (app_config['name'] in cur_apps) and (not overwrite):
            msg = f"Unable to deploy app {app_config['name']} - already exists and overwite is not set."
            logger.info(msg)
            raise Exception(msg)

        try:
            # Now try and send application data and config to system
            local_app_dir = os.path.join(local_app_dir, 'assets')
            remote_app_dir = '/'.join([self.apps_dir, app_config['name']])
            self.send_data(local_app_dir, remote_app_dir)

            # Put app config in deployed app folder
            app_config_path = '/'.join([remote_app_dir, 'app.json'])
            with self._client.open_sftp() as sftp:
                with sftp.open(app_config_path, 'w') as jc:
                    json.dump(app_config, jc)
        except Exception as e:
            msg = f"Unable to stage appplication data to path {app_config_path}."
            logger.error(msg)
            raise e

        return app_config


    def get_jobs(self):
        jobs = self.list_files(path=self.jobs_dir)

        return jobs


    def get_job(self, job_name):
        try:
            job_config = '/'.join([self.jobs_dir, job_name, 'job_config.json'])
            with self._client.open_sftp() as sftp:
                with sftp.open(job_config, 'rb') as jc:
                    job_config = json.load(jc)
        except Exception as e:
            msg = f"Unable to load {job_name}"
            logger.error(msg)
            raise e

        return job_config


    def stage_job(self, job_id, **job_configs):
        # New job config
        job_config = {}

        # Check if job has been initialized yet
        if job_id not in self.get_jobs():
            # Passed in key word arguments are the base inputs
            job_config = job_configs

            # Create job directory in job manager's jobs folder
            job_config['job_dir'] = f"{self.jobs_dir}/{job_id}"
            ret = self._execute_command('mkdir ' + job_config['job_dir'])

            # Copy app contents to job directory
            cmnd = 'cp -r {apps_dir}/{app}/* {job_dir}/'.format(apps_dir=self.apps_dir,
                    app=job_config['appId'], job_dir=job_config['job_dir'])
            ret = self._execute_command(cmnd)
        else:
            # Pull current job config and update values with values in new dictionary
            new_job_config = self.get_job(job_id)
            new_job_config.update(job_configs)
            job_config = new_job_config

        # Set (or overwrite) job id
        job_config['job_id'] = job_id

        # Get app config for job and wrapper script
        app_config = self.get_app(job_config['appId'])
        wrapper_script = app_config['wrapper_script']

        # Helper function get attributes for job from app defaults if not present in job config
        def _get_attr(j, a):
            if j in job_config.keys():
                return job_config[j]
            else:
                return app_config[a]

        job_config['entry_script'] = _get_attr('entry_script', 'templatePath')
        job_config['desc'] = _get_attr('desc','shortDescription')
        job_config['queue'] = _get_attr('queue','defaultQueue')
        job_config['nodeCount'] = _get_attr('nodeCount','defaultNodeCount')
        job_config['processorsPerNode'] = _get_attr('processorsPerNode','defaultProcessorsPerNode')
        job_config['maxRunTime'] = _get_attr('maxRunTime','defaultMaxRunTime')

        # Format submit scripts with appropriate inputs for job
        submit_script = self.SUBMIT_SCRIPT_TEMPLATE.format(job_name=job_config['name'],
                job_desc=job_config['desc'],
                job_id=job_config['job_id'],
                queue=job_config['queue'],
                N=job_config['nodeCount'],
                n=job_config['processorsPerNode'],
                rt=job_config['maxRunTime'])

        # submit script - add slurm directives for email and allocation if specified for job
        if 'email' in job_config.keys():
            submit_script += "\n#SBATCH --mail-user={email} # Email to send to".format(
                    email=job_config['email'])
            submit_script += "\n#SBATCH --mail-type=all     # Email to send to"
        # TODO: Check if allocation is needed first - Throw error if it is
        if 'allocation' in job_config.keys():
            submit_script += "\n#SBATCH -A {allocation} # Allocation name ".format(
                    allocation=job_config['allocation'])

        # End slurm directives and cd into job dir
        submit_script += "\n#----------------------------------------------------\n"
        submit_script += f"\ncd {job_config['job_dir']}\n"

        # submit script - parse line to invoke wrapper.sh script that starts off application.
        execute_line = f"\n{job_config['job_dir']}/{job_config['entry_script']}.sh "

        # Add preamble to parse arguments to wrapper script
        wrapper_preamble = "# Create start ts file\ntouch start_$(date +\"%FT%H%M%S\")\n"
        wrapper_preamble += "\n# Parse arguments passed\nfor ARGUMENT in \"$@\"\ndo\n"
        wrapper_preamble += "\n    KEY=$(echo $ARGUMENT | cut -f1 -d=)"
        wrapper_preamble += "\n    VALUE=$(echo $ARGUMENT | cut -f2 -d=)\n"
        wrapper_preamble += "\n    case \"$KEY\" in"

        # NP, the number of mpi processes available to job, is always a variable passed
        wrapper_preamble += "\n        NP)           NP=${VALUE} ;;"
        execute_line += " NP=" + str(job_config['processorsPerNode'])

        # Transfer inputs to job directory
        for arg in job_config['inputs'].keys():
            path = job_config['inputs'][arg]

            dest_path = '/'.join([job_config['job_dir'], os.path.basename(path)])
            try:
                self.send_data(path, dest_path)
            except Exception as e:
                msg = f"Unable to send input file {path} to dest {dest_path}"
                logger.error(msg)
                raise e

            # Pass in path to input file as argument to application
            # pass name,value pair to application wrapper.sh
            execute_line += f" {arg}={dest_path}"
            wrapper_preamble += "\n        {arg})           {arg}=${{VALUE}} ;;".format(arg=arg)

        # Add on parameters passed to job
        for arg in job_config['parameters'].keys():
            value = job_config['parameters'][arg]

            # pass name,value pair to application wrapper.sh
            execute_line += f" {arg}={value}"
            wrapper_preamble += f"\n        {arg})           {arg}=${{VALUE}} ;;"

        # Close off wrapper preamble
        wrapper_preamble += "\n        *)\n    esac\n\ndone\n\n"

        # submit script - add execution line to wrapper.sh
        submit_script += execute_line + '\n'

        # Line to create end ts
        wrapper_post = "\n# Create end ts file\ntouch end_$(date +\"%FT%H%M%S\")\n"

        # Stage wrapper and submit scripts to job directory
        with self._client.open_sftp() as sftp:
            submit_dest = '/'.join([job_config['job_dir'], 'submit_script.sh'])
            wrapper_dest = '/'.join([job_config['job_dir'], job_config['entry_script']])
            with sftp.open(submit_dest,  'w') as ss_file:
                ss_file.write(submit_script)
            with sftp.open(wrapper_dest, 'w') as ws_file:
                ws_file.write(wrapper_preamble + wrapper_script + wrapper_post)

        # chmod submit_scipt and wrapper script to make them executables
        self._execute_command('chmod +x ' + submit_dest)
        self._execute_command('chmod +x ' + wrapper_dest)

        # Save job config
        jc_path = job_config['job_dir'] + '/job_config.json'
        self.send_json(job_config, jc_path)

        return job_config


    def setup_job(self, local_job_dir='.', job_config_file='job.json',
            proj_config_file="project.ini", stage=True):

        # Load project configuration file
        proj_config_path = os.path.join(local_job_dir, proj_config_file)
        proj_config = self.load_project_config(proj_config_path)

        # Load templated job configuration -> Default is job.json
        job_config_path = os.path.join(local_job_dir, job_config_file)
        job_config = self.load_templated_json_file(job_config_path,  proj_config)

        # Add ts of setup to create job_id
        job_id = '{job_name}_{ts}'.format(job_name=job_config['name'],
                ts=datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))

        # Now stage job inputs
        if stage:
            try:
                job_config = self.stage_job(job_id, **job_config)
            except Exception as e:
                job_dir = '/'.join([self.jobs_dir, job_id])
                msg = f"Error staging job {job_id}. Cleaning up job directory {job_dir}."
                logger.error(msg)
                _ = self._execute_command(f"rm -rf {job_dir}")
                raise e

        return job_config


    def submit_job(self, job_id):
        # Load job config
        job_config =  self.get_job(job_id)

        # Check it hasn't been submitted already
        if 'slurm_id' in job_config.keys():
            raise Exception(f"{job_id} already submitted - job id = {job_config['slurm_id']}")

        # Submit to SLURM queue
        cmnd = f"cd {job_config['job_dir']};sbatch submit_script.sh"
        ret = self._execute_command(cmnd)
        slurm_ret = ret.split('\n')[-2]
        if 'errror' in slurm_ret or 'FAILED' in slurm_ret:
            raise Exception(f"Failed to submit job. SLURM returned error: {slurm_ret}")
        job_config['slurm_id'] = slurm_ret.split(' ')[-1]

        # Save job config
        jc_path = job_config['job_dir'] + '/job_config.json'
        self.send_json(job_config, jc_path)

        return job_config


    def cancel_job(self, job_id):
        # Load job config
        job_config =  self.get_job(job_id)

        if 'slurm_id' in job_config.keys():
            cmnd = f"scancel {job_config['slurm_id']}"

            try:
                self._execute_command(cmnd)
            except Exception as e:
                msg = f"Failed to cancel job {job_id}."
                logger.error(msg)
                raise e

            # Remove slurm ID
            _ = job_config.pop('slurm_id')

            # Save job config
            jc_path = job_config['job_dir'] + '/job_config.json'
            self.send_json(job_config, jc_path)
        else:
            msg = f"Job {job_id} has not been submitted yet."
            logger.error(msg)
            raise Exception(msg)

        return job_config


    def cleanup_job(self, job_id):
        # Cancel job
        try:
            self.cancel_job(job_id)
        except:
            pass

        # Remove job directory
        job_dir = '/'.join([self.jobs_dir, job_id])
        cmnd = f"rm -r {job_dir}"
        try:
            self._execute_command(cmnd)
        except:
            pass

        return job_id


    def ls_job(self, job_id, path=''):
        # Get files from particular directory in job
        fpath ='/'.join([self.jobs_dir, job_id, path])
        files = self.list_files(path=fpath)

        return files


    def get_job_file(self, job_id, fpath, dest_dir='.'):
        # Downlaod to local job dir
        fpath = fpath[:-1] if fpath[-1]=='/' else fpath
        fname = '/'.join(fpath.split('/')[-1:])
        job_folder = '/'.join(fpath.split('/')[:-1])

        # Make sure job file/folder exists
        files = self.ls_job(job_id, path=job_folder)
        if fname not in files:
            msg = f"Unable to find job file {fpath} for job {job_id}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fpath)

        # Make local data directory if it doesn't exist already
        local_data_dir = os.path.join(dest_dir, job_id)
        os.makedirs(local_data_dir, exist_ok=True)

        # Get file
        src_path = '/'.join([self.jobs_dir, job_id, fpath])
        dest_path = os.path.join(local_data_dir, fname)
        try:
            self.get_file(src_path, dest_path)
        except Exception as e:
            msg = f"Unable to download job file {src_path} to {dest_path}"
            logger.error(msg)
            raise Exception(msg)
        return dest_path


    def send_job_file(self, job_id, fpath, dest_dir='.'):
        try:
            # Get destination directory in job path to send file to
            fname = os.path.basename(os.path.normpath(fpath))
            dest_path = '/'.join([self.jobs_dir, job_id, dest_dir, fname])

            self.send_data(fpath, dest_path)
        except Exception as e:
            msg = f"Unable to send file {fpath} to destination destination {dest_path}"
            logger.error(msg)
            raise Exception(msg)
        return dest_path


    def peak_job_file(self, job_id, fpath, head=-1, tail=-1):
        # Load job config
        path = '/'.join([self.jobs_dir, job_id, fpath])

        return self.peak_file(path, head=head, tail=tail)

