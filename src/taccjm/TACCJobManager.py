"""
TACCJobManager Class


"""

# TODO: Investigate how to handle closing of paramiko ssh client adequately

from taccjm import __version__  # Version of this library

import os                       # OS system utility functions -> for local system
import posixpath                # Path manipulation on remote system (assumed UNIX)
import errno                    # For error messages
import tarfile                  # For sending compressed directories
import fnmatch                  # For unix-style filename pattern matching
import re                       # Regular Expressions
import pdb                      # Debug
import json                     # For saving and loading job configs to disk
import time                     # Time functions
import logging                  # Used to setup the Paramiko log file
import datetime                 # Date time functionality
import stat                     # For determining if remote paths are directories
from taccjm.utils import *      # TACCJM Util functions for config files/dicts

# Modified paramiko ssh client and common paramiko exceptions
from taccjm.SSHClient2FA import SSHClient2FA
from paramiko import SSHException, AuthenticationException, BadHostKeyException

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


logger = logging.getLogger(__name__)


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
        msg =  f"{self.message}"
        msg += f"\n{self.user}@{self.system}$ {self.command}"
        msg += f"\nrc     : {self.rc}"
        msg += f"\nstdout : {self.stdout}"
        msg += f"\nstderr : {self.stderr}"
        return msg


class TACCJobManager():
    """
    Class defining an ssh connection to a TACC resource. Note path's to resources are all
    unix-stype paths and are relative to the job manager's base directory.

    Attributes
    ----------
    system : str
        Name of tacc system to connect to. Supported systems:
        stampede2, ls5, frontera, maverick2
    user : str
        Name of tacc user connecting via ssh to resource.
    jobs_dir : str
        Unix-stlye path to directory where jobs for job manager can be found.
    apps_dir : str
        Unix-style path to directory where applications for job manager can be found.
    scripts_dir : str
        Unix-stye path to directory where scripts for job manager can be found.

    Methods
    -------

    """

    TACC_SYSTEMS = ['stampede2', 'ls5', 'frontera', 'maverick2']
    TACC_USER_PROMPT = "Username:"
    TACC_PSW_PROMPT = "Password:"
    TACC_MFA_PROMPT ="TACC Token Code:"
    SCRATCH_DIR = "$SCRATCH"
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


    def __init__(self, system, user=None, psw=None, mfa=None, working_dir='taccjm'):
        """
        Create a new TACC Job Manager instance for apps/jobs on given TACC system.

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
        working_dir: str , default='taccjm'
            Unix-style path relative to user's SCRATCH directory to place all data related to this
            job manager instance. This includes the apps, jobs, scripts, and trash directories.
        """

        if system not in self.TACC_SYSTEMS:
            msg = f"Unrecognized TACC system {system}. Must be one of {self.TACC_SYSTEMS}."
            logger.error(msg)
            raise ValueError(msg)
        if any([working_dir.startswith('../'),
                working_dir.endswith('/..'),
                '/../' in working_dir ]):
            msg = f"Inavlid working directory {working_dir}. Contains '..' which isn't allowed."
            logger.error(msg)
            raise ValueError(msg)

        # Connect to system
        logger.info(f"Connecting {user} to TACC system {system}...")
        self.system= f"{system}.tacc.utexas.edu"
        self._client = SSHClient2FA(user_prompt=self.TACC_USER_PROMPT,
                psw_prompt=self.TACC_PSW_PROMPT,
                mfa_prompt=self.TACC_MFA_PROMPT)
        self._client.load_system_host_keys()
        self.user = self._client.connect(self.system, uid=user, pswd=psw, mfa_pswd=mfa)
        logger.info(f"Succesfuly connected to {system}")

        # Get taccjm working directory, relative to users scratch directory
        logger.info("Resolving SCRATCH directory path ${self.SCRATCH_DIR} for user {self.user}")
        self.scratch_dir = scratch_dir = self._execute_command(f"echo {self.SCRATCH_DIR}").strip()
        taccjm_dir = posixpath.join(scratch_dir, working_dir)

        # Initialze jobs, apps, scripts, and trash dirs
        logger.info("Creating if jobs, apps, sripts, and trash dirs if they don't already exist")
        for d in zip(['jobs_dir', 'apps_dir', 'scripts_dir', 'trash_dir'],
                      ['jobs', 'apps', 'scripts', 'trash']):
            setattr(self, d[0], posixpath.join(taccjm_dir,d[1]))
            self._mkdir(getattr(self, d[0]), parents=True)


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
        except SSHException as ssh_error:
            # Will only occur if ssh connection is broken
            msg = "Unalbe to excecute command. TACCJM ssh connection error: {ssh_error.__str__()}"
            logger.error(msg)
            raise ssh_error

        out = stdout.read().decode('utf-8')
        err = stderr.read().decode('utf-8')
        rc = stdout.channel.recv_exit_status()

        if rc!=0:
            # Build base TJMCommand Error, only place this should be done
            t = TJMCommandError(self.system, self.user, cmnd, rc, out, err)

            # Only log the actual TJMCommandError object once, here
            logger.error(t.__str__())

            raise t

        return out


    def _mkdir(self, path, parents=False):
        """
        Creates directory on remote system.

        Parameters
        ----------
        path: str
            Unix-stype path on remote system to create.

        Returns
        -------
        err_code : int
            An SFTP error code int like SFTP_OK (0).

        """
        cmnd = f"mkdir {path}" if not parents else f"mkdir -p {path}"

        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "_mkdir - Could not create directory"
            logger.error(tjm_error.message)
            raise tjm_error


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
            cmnd += f"-U {user}"

        # Query job queue
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "showq - TACC SLURM queue is not accessible."
            logger.error(tjm_error.message)
            raise tjm_error

        # Loop through lines in output table and parse job information
        jobs = []
        parse = lambda x : {'job_id':x[0], 'job_name':x[1], 'username':x[2], 'state':x[3],
                 'nodes':x[4], 'remaining': x[4], 'start_time': x[5]}
        lines = ret.split('\n')
        jobs_line = False
        line_counter = -2
        for l in lines:
            if any([l.startswith(x) for x in ['ACTIVE', 'WAITING', 'BLOCKED', 'COMPLETING']]):
                jobs_line=True
                continue
            if jobs_line==True:
                if l=='':
                    jobs_line = False
                    line_counter = -2
                    continue
                else:
                    line_counter += 1
                    if line_counter>0:
                        jobs.append(parse(l.split()))

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
        cmnd = '/usr/local/etc/taccinfo'
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
        except TJMCommandError as t:
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


    def upload(self, local, remote, file_filter='*'):
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
            If a folder is being uploaded, unix style pattern matching string to use on
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
        remote_dir, remote_fname = os.path.split(remote)
        remote_dir = '.' if remote_dir=='' else remote_dir

        try:
            # Sending directory -> Package into tar file
            if os.path.isdir(local):
                fname = os.path.basename(local)
                local_tar_file = f".{fname}.taccjm.tar"
                remote_tar_file = f"{remote_dir}/.taccjm_temp_{fname}.tar"

                # Package tar file -> Recursive call
                with tarfile.open(local_tar_file, "w:gz") as tar:
                    f = lambda x : x if fnmatch.fnmatch(x.name, file_filter) else None
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
            msg = f"upload - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"upload - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except TJMCommandError as t:
            t.message = f"upload - Error unpacking tar file in remote directory"
            logger.error(t.message)
            raise t
        except Exception as e:
            msg = f"upload - Unexpected error {e.__str__()}"
            logger.error(msg)
            raise e


    def download(self, remote, local, file_filter='*'):
        """
        Downloads file or folder from remote path on TACC resource to local path. If a
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

        local = local.rstrip('/')
        remote = remote.rstrip('/')
        try:
            with self._client.open_sftp() as sftp:
                fileattr = sftp.stat(remote)
                is_dir = stat.S_ISDIR(fileattr.st_mode)
                if is_dir:
                    dirname, fname = os.path.split(remote)
                    self._execute_command(f"cd {dirname} && tar -czvf {fname}.tar.gz {fname}")
                    local_dir = os.path.abspath(os.path.join(local, os.pardir))
                    local = f"{local_dir}/{fname}.tar.gz"
                    remote = f"{dirname}/{fname}.tar.gz"
                sftp.get(remote, local)
                if is_dir:
                    with tarfile.open(local) as tar:
                        tar.extractall(path=os.path.dirname(local))
                    os.remove(local)
        except FileNotFoundError as f:
            msg = f"download - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"download - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)


    def remove(self, remote_path):
        """
        'Removes' a file/folder by moving it to the trash directory. Trash should be emptied
        out preiodically with `empty_trash()` method. Can also restore file `restore(path)` method.


        Parameters
        ----------
        remote_path : str
            Unix-style path for the file/folder to send to trash. Relative to home directory
            for user on TACC system if not an absolute path.

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
        file_name = remote_path.replace('/','___')
        trash_path = f"{self.trash_dir}/{file_name}"
        abs_remote_path = '~/' + remote_path if remote_path[0]!='/' else remote_path

        cmnd = f"mv {abs_remote_path} {trash_path}"
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = f"remove - Unable to remove {remote_path}"
            logger.error(tjm_error.message)
            raise tjm_error


    def restore(self, remote_path):
        """
        Restores a file/folder from the trash directory by moving it back to its original path.

        Parameters
        ----------
        remote_path : str
            Unix-style path of the file/folder that should not exist anymore to restore.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If the `remote_path` to restore does not exist in trash directory anymore, or loacation:
            where remote_path needs to be restored to does not exist anymore.
        """
        # Unix paths -> Get file remote file name and directory
        file_name = remote_path.replace('/','___')
        trash_path = f"{self.trash_dir}/{file_name}"
        abs_remote_path = '~/' + remote_path if remote_path[0]!='/' else remote_path

        cmnd = f"mv {trash_path} {abs_remote_path}"
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = f"remove - Unable to restore {remote_path}"
            logger.error(tjm_error.message)
            raise tjm_error


    def send_data(self, data, path):
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
        if d_type not in [dict, str]:
            raise ValueError(f"Data type {d_type} is not supported")
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(path, 'w') as jc:
                    if d_type==dict:
                        json.dump(data, jc)
                    else:
                        jc.write(data)
        except FileNotFoundError as f:
            msg = f"send_data - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"send_data - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except Exception as e:
            msg = f"sed_data - Unknown error when trying to write {d_type} data to {path}: {e}"
            logger.error(msg)
            raise e


    def get_data(self, path, data_type='text'):
        """
        Get data of `data_type` in file `path` on remote TACC system directly
        via a file stream. Supported data types are:
            1. text -> str (Default)
            2. json -> dict

        Parameters
        ----------
        path : str
            Unix-style path on TACC system containing desired data.
        data_type : str, optional, Default = 'text'
            Type of data to get from desired file. Currently only 'text'
            and 'json' data types are supported.

        Returns
        -------
        data : str, dict
            Either text or dictionary containing data stored on remote system.

        Raises
        -------
        ValueError
            If invalid data type specified.

        """
        if data_type not in ['json', 'text']:
            raise ValueError(f"get_data - data type {data_type} is not supported")
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(path, 'r') as fp:
                    if data_type=='json':
                        data = json.load(fp)
                    else:
                        data = fp.read().decode('UTF-8')
            return data
        except FileNotFoundError as f:
            msg = f"get_data - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"get_data - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except Exception as e:
            msg = f"get_data - Unknown error when trying to get {data_type} data from {path}: {e}"
            logger.error(msg)
            raise e


    def get_apps(self):
        """
        Get list of applications deployed by TACCJobManager instance.

        Parameters
        ----------
        None

        Returns
        -------
        apps : list of str
            List of applications contained in the apps_dir for this TACCJobManager instance.

        """
        apps = self.list_files(path=self.apps_dir)

        return apps


    def get_app(self, app_id):
        """
        Get application config for app deployed at TACCJobManager.apps_dir.

        Parameters
        ----------
        app_id : str
            Name of application to pull config for. Must exist in TACCJobManager apps_dir.
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
            logger.error(msg)
            raise ValueError(msg)

        # Load application config
        app_config_path = '/'.join([self.apps_dir, app_id, 'app.json'])
        app_config = self.get_data(app_config_path, data_type='json')

        return app_config


    def deploy_app(self, app_config=None, local_app_dir='.',
            app_config_file="app.json", proj_config_file="project.ini",
            overwrite=False, **kwargs):
        """
        Deploy local application to TACCJobManager.apps_dir. Values in project config file
        are substituted in where needed in the app config file to form application config,
        and then app contents in assets directory (relative to local_app_dir) are sent to
        to the apps_dir along with the application config (as a json file).

        Parameters
        ----------
        app_config : dict, default=None
            Dictionary containing app config. If None specified, then app config will be read from
            file specified at local_app_dir/app_config_file, with templated arguments filled in
            from the project config file at local_app_dir/proj_config_file
        local_app_dir: str, default='.'
            Directory containing application to deploy.
        app_config_file: str, default='app.json'
            Path relative to local_app_dir containing application config json file.
        proj_config_file: str, default="project.ini"
            Path relative to local_app_dir containing project .ini config file.
        overwrite: bool, default=False
            Whether to overwrite application if it already exists in application directory.
        **kwargs : dict, optional
            All extra keyword arguments will be interpreted as items to
            override in app config found in json file.
        ----------

        Returns
        -------
        app_config : dict
            Application config dictionary as stored in application directory.

        Raises
        ------
        ValueError
            If app_config is missing a required field or application already exists but overwrite
            is not set to True.

        """

        # Load templated app configuration
        if app_config is None:
            app_config_path = os.path.join(local_app_dir, app_config_file)
            proj_config_path = os.path.join(local_app_dir, proj_config_file)
            app_config = load_templated_json_file(app_config_path,
                    proj_config_path, **kwargs)

        # Get current apps already deployed
        cur_apps = self.get_apps()

        # Required parameters for application configuration
        required_args = ['name', 'shortDescription', 'defaultQueue', 'defaultNodeCount',
                'defaultProcessorsPerNode', 'defaultMaxRunTime', 'templatePath', 'inputs',
                'parameters', 'outputs']
        missing = set(required_args) - set(app_config.keys())
        if len(missing)>0:
            msg = f"deploy_app - missing required app configs {missing}"
            logger.error(msg)
            raise ValueError(msg)

        # Only overwrite previous version of app (a new revision) if overwrite is set.
        if (app_config['name'] in cur_apps) and (not overwrite):
            msg = f"deploy_app - {app_config['name']} already exists and overwite is not set."
            logger.info(msg)
            raise ValueError(msg)

        try:
            # Now try and send application data and config to system
            local_app_dir = os.path.join(local_app_dir, 'assets')
            remote_app_dir = '/'.join([self.apps_dir, app_config['name']])
            self.upload(local_app_dir, remote_app_dir)

            # Put app config in deployed app folder
            app_config_path = '/'.join([remote_app_dir, 'app.json'])
            self.send_data(app_config, app_config_path)

            # Make entry point script executable
            wrapper_script_path = f"{remote_app_dir}/{app_config['templatePath']}"
            self._execute_command(f"chmod +x {wrapper_script_path}")

        except Exception as e:
            msg = f"deploy_app - Unable to stage appplication data."
            logger.error(msg)
            raise e

        return app_config


    def get_jobs(self):
        """
        Get list of all jobs in TACCJobManager jobs directory.

        Parameters
        ----------
        None

        Returns
        -------
        jobs : list of str
            List of jobs contained in the jobs_dir for this TACCJobManager instance.

        """
        jobs = self.list_files(path=self.jobs_dir)

        return jobs


    # TODO: Catch errors -> Common one such as if job doesn't exist
    def get_job(self, jobId):
        """
        Get job config for job in TACCJobManager jobs_dir.

        Parameters
        ----------
        jobId: str
            ID of job to get (should be same as name of job directory in jobs_dir).
        ----------

        Returns
        -------
        job_config: dict
            Job config dictionary as stored in json file in job directory.

        Raises
        ------

        """
        try:
            job_config_path = '/'.join([self.jobs_dir, jobId, 'job.json'])
            return self.get_data(job_config_path, data_type='json')
        except Exception as e:
            msg = f"get_job - Unable to load {jobId}"
            logger.error(msg)
            raise e


    # TODO: Check for valid job config, document more
    def parse_submit_script(self, job_config):
        """
        Parses text to write for a SLURM job submission script on TACC.

        Parameters
        ----------
        job_config: dict
            Dictionary containing job configurations.
        ----------

        Returns
        -------
        submit_script: str
            String containing text to write to submit script.

        Raises
        ------

        """

        # ---- HEADER ---- 
        # The total number of MPI processes is the product of these two quantities
        total_mpi_processes = job_config['nodeCount'] * job_config['processorsPerNode']

        # Format submit scripts with appropriate inputs for job
        submit_script_header = self.SUBMIT_SCRIPT_TEMPLATE.format(job_name=job_config['name'],
                job_desc=job_config['desc'],
                job_id=job_config['job_id'],
                queue=job_config['queue'],
                N=job_config['nodeCount'],
                n=total_mpi_processes,
                rt=job_config['maxRunTime'])

        # submit script - add slurm directives for email and allocation if specified for job
        if 'email' in job_config.keys():
            submit_script_header += "\n#SBATCH --mail-user={email} # Email to send to".format(
                    email=job_config['email'])
            submit_script_header += "\n#SBATCH --mail-type=all     # Email to send to"
        # TODO: Check if allocation is needed first - Throw error if it is
        if 'allocation' in job_config.keys():
            submit_script_header += "\n#SBATCH -A {allocation} # Allocation name ".format(
                    allocation=job_config['allocation'])

        # End slurm directives and cd into job dir
        submit_script_header += "\n#----------------------------------------------------\n"

        # ---- ARGS ---- 

        # always pass total number of MPI processes
        job_args = {"NP": job_config['nodeCount'] * job_config['processorsPerNode']}

        # Add paths to job inputs as argument 
        for arg, path in job_config['inputs'].items():
            job_args[arg] = '/'.join([job_config['job_dir'], os.path.basename(path)])

        # Add on parameters passed to job 
        job_args.update(job_config['parameters'])

        # Create list of arguments to pass as env variables to job
        export_list = [""]
        for arg, value in job_args.items():
            value = str(value)
            # wrap with single quotes if needed
            if " " in value and not (value[0] == value[-1] == "'"):
                value = f"'{value}'"
            export_list.append(f"export {arg}={value}")

        # Parse final submit script 
        submit_script = (
            submit_script_header                                            # set SBATCH params
            + f"\ncd {job_config['job_dir']}\n\n"                           # cd to job directory
            + "\n".join(export_list)                                        # set job params
            + f"\n{job_config['job_dir']}/{job_config['entry_script']}.sh " # run main script
        )

        return submit_script


    # TODO: Check for required job configurations. Document job_config fields
    def setup_job(self, job_config=None, local_job_dir='.',
            job_config_file='job.json', proj_config_file="project.ini", stage=True, **kwargs):
        """
        Setup job directory on supercomputing resources. If job_config is not specified, then it is
        parsed from the json file found at local_job_dir/job_config_file, with jinja templated
        values from the local_job_dir/proj_config_file substituted in accordingly. In either case,
        values found in dictionary or in parsed json file can be overrided by passing keyword
        arguments. Note for dictionary values, only the specific keys in the dictionary value
        specified will be overwritten in the existing dictionary value, not the whole dictionary.

        Parameters
        ----------
        job_config : dict, default=None
            Dictionary containing job config. If None specified, then job config will be read from
            file specified at local_job_dir/job_config_file.
        local_job_dir : str, default='.'
            Local directory containing job config file and project config file. Defaults to cwd.
        job_config_file : str, default='job.json'
            Path, relative to local_job_dir, to job config json file. File only read if job_config
            dictionary not given.
        proj_config_file : str, default='project.ini'
            Path, relative to local_job_dir, to project config .ini file. Only used if job_config
            not specified. If used, jinja is used to substitue values found in config file into
            the job json file. Useful for templating jobs.
        stage : bool, default=False
            If set to True, stage job directory by creating it, moving application contents,
            moving job inputs, and writing submit_script to remote system.
        kwargs : dict, optional
            All extra keyword arguments will be interpreted as job config overrides.


        Returns
        -------
        job_config : dict
            Dictionary containing info about job that was set-up. If stage was set to True,
            then a successful completion of setup_job() indicates that the job directory was
            prepared succesffuly and job is ready to be submit.

        Raises
        ------
        """
        # Load from json file if job conf dictionary isn't specified
        # Overwrite job_config loaded with kwargs keyword arguments if specified
        if job_config is None:
            job_config_path = os.path.join(local_job_dir, job_config_file)
            proj_config_path = os.path.join(local_job_dir, proj_config_file)
            job_config = load_templated_json_file(job_config_path, proj_config_path, **kwargs)
        else:
            job_config = update_dic_keys(job_config, **kwargs)

        if job_config.get('job_id') is None or job_config.get('job_dir') is None:
            # job_id is name of job folder in jobs_dir
            job_config['job_id'] = '{job_name}_{ts}'.format(job_name=job_config['name'],
                    ts=datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S'))
            job_config['job_dir'] = '{job_dir}/{job_id}'.format(job_dir=self.jobs_dir,
                    job_id=job_config['job_id'])
            job_dir_exists = False
        else:
            job_dir_exists = True

        # Get default arguments from deployed application
        app_config = self.get_app(job_config['appId'])
        def _get_attr(j, a):
            # Helper function get attributes for job from app defaults if not present in job config
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

        # TODO: check all necessary job arguments present?

        if stage:
            # Stage job inputs -> Actually transfer/write job data to remote system
            try:
                # Make job directory
                job_dir = job_config['job_dir']
                if not job_dir_exists:
                    sftp_ret = self._mkdir(job_dir)
                    # TODO: Check return of mkdir?

                # Copy app contents to job directory
                cmnd = 'cp -r {apps_dir}/{app}/* {job_dir}/'.format(apps_dir=self.apps_dir,
                        app=job_config['appId'], job_dir=job_dir)
                ret = self._execute_command(cmnd)

                # Send job input data to job directory
                for arg, path in job_config['inputs'].items():
                    self.upload(path, '/'.join([job_dir, os.path.basename(path)]))

                # Parse and write submit_script to job_directory
                submit_script = self.parse_submit_script(job_config)
                submit_script_path = f"{job_dir}/submit_script.sh"
                self.send_data(submit_script, submit_script_path)

                # chmod submit_scipt 
                self._execute_command(f"chmod +x {submit_script_path}")

                # Save job config
                job_config_path = f"{job_dir}/job.json"
                self.send_data(job_config, job_config_path)

            except Exception as e:
                # If failed to stage job, remove contents that have been staged
                self._execute_command('rm -rf ' + job_dir)
                message = f"stage_job - Error staging: {e}."
                logger.error(message)
                raise e


        return job_config


    def submit_job(self, jobId):
        """
        Submit job to remote system job queue.

        Parameters
        ----------
        jobId : str
            ID of job to submit.

        Returns
        -------
        job_config : dict
            Dictionary containing information on job just submitted. The new field 'slurm_id'
            should be added populated with id of job.

        Raises
        ------
        """
        # Load job config
        job_config =  self.get_job(jobId)

        # TODO: Verify job is set-up properly? 

        # Check if this job isn't currently in the queue
        if 'slurm_id' in job_config.keys():
            msg = f"submit_job - {jobId} exists in queue with id {job_config['slurm_id']}"
            raise ValueError(msg)

        # Submit to SLURM queue -> Note we do this from the job_directory
        cmnd = f"sbatch {job_config['job_dir']}/submit_script.sh"
        ret = self._execute_command(cmnd)
        slurm_ret = ret.split('\n')[-2]
        if 'error' in slurm_ret or 'FAILED' in slurm_ret:
            raise Exception(f"Failed to submit job. SLURM returned error: {slurm_ret}")
        job_config['slurm_id'] = slurm_ret.split(' ')[-1]

        # Save job config
        job_config_path = job_config['job_dir'] + '/job.json'
        self.send_data(job_config, job_config_path)

        return job_config


    def cancel_job(self, jobId):
        """
        Cancel job on remote system job queue.

        Parameters
        ----------
        jobId : str
            ID of job to submit.

        Returns
        -------
        job_config : dict
            Dictionary containing information on job just canceled. The field 'slurm_id' should
            be removed from the job_config dictionary and the 'slurm_hist' field should be
            populated with a list of previous slurm_id's with the latest one appended. Updated
            job config is also updated in the jobs directory.
        """
        # Load job config
        job_config =  self.get_job(jobId)

        if 'slurm_id' in job_config.keys():
            cmnd = f"scancel {job_config['slurm_id']}"
            try:
                self._execute_command(cmnd)
            except Exception as e:
                msg = f"Failed to cancel job {jobId}."
                logger.error(msg)
                raise e

            # Remove slurm ID and store into job hist
            old_slurm_id = job_config.pop('slurm_id')
            job_config['slurm_hist'] = job_config.get('slurm_hist', []).append(old_slurm_id)

            # Save updated job config
            job_config_path = job_config['job_dir'] + '/job.json'
            self.send_data(job_config, job_config_path)
        else:
            msg = f"Job {jobId} has not been submitted yet."
            logger.error(msg)
            raise Exception(msg)

        return job_config


    def cleanup_job(self, jobId):
        """
        Cancels job if it has been submitted to the job queue and deletes the job's directory.

        Parameters
        ----------
        jobId: str
            Job ID of job to clean up.

        Returns
        -------
        jobId: str
            Job ID of job just canceled and removed.
        """
        # Cancel job
        try:
            self.cancel_job(jobId)
            job_dir = '/'.join(self.jobs_dir, jobId)
            self.remove(job_dir)
        except:
            pass

        return jobId


    def ls_job(self, jobId, path=''):
        """
        List files in job directory with given ID.

        Parameters
        ----------
        jobId: str
            ID of job.
        path: str, default=''
            Directory, relative to the job directory jobs_dir/jobId, to to get files for.


        Returns
        -------
        files : list of str
            List of files in job directory.
        """
        # Get files from particular directory in job
        fpath ='/'.join([self.jobs_dir, jobId, path])
        files = self.list_files(path=fpath)

        return files


    def download_job_data(self, jobId, path, dest_dir='.'):
        """
        Download file/folder at path, relative to job directory, and place it in the specified local
        destination directory.

        Parameters
        ----------
        jobId : str
            ID of job.
        path : str
            Path to file/folder, relative to the job directory jobs_dir/jobId, to download. If a
            folder is specified, contents are compressed, sent, and then decompressed locally.
        dest_dir :  str, default='.'
            Local directory to download job data to. Defaults to current working directory.


        Returns
        -------
        dest_path : str
            Local path of file/folder downloaded.
        """
        # Downlaod to local job dir
        path = path[:-1] if path[-1]=='/' else path
        fname = '/'.join(path.split('/')[-1:])
        job_folder = '/'.join(path.split('/')[:-1])

        # Make sure job file/folder exists
        files = self.ls_job(jobId, path=job_folder)
        if fname not in files:
            msg = f"Unable to find job file {path} for job {jobId}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        # Make local data directory if it doesn't exist already
        local_data_dir = os.path.join(dest_dir, jobId)
        os.makedirs(local_data_dir, exist_ok=True)

        # Get file
        src_path = '/'.join([self.jobs_dir, jobId, path])
        dest_path = os.path.join(local_data_dir, fname)
        try:
            self.download(src_path, dest_path)
        except Exception as e:
            msg = f"Unable to download job file {src_path} to {dest_path}"
            logger.error(msg)
            raise Exception(msg)
        return dest_path


    def get_job_data(self, jobId, path, data_type='text'):
        """
        Download file/folder at path, relative to job directory, and place it in the specified local
        destination directory.
        Get data of `data_type` in file `path`, relative to job directory on remote TACC system,
        directly via a file stream. Supported data types are:
            1. text -> str (Default)
            2. json -> dict

        Parameters
        ----------
        jobId : str
            ID of job.
        path : str
            Unix-style path, relative to job directory on TACC system, containing desired data.
        data_type : str, optional, Default = 'text'
            Type of data to get from desired file. Currently only 'text'
            and 'json' data types are supported.

        Returns
        -------
        data : str, dict
            Either text or dictionary containing data stored on remote system.

        Raises
        -------
        ValueError
            If invalid data type specified.
        """
        # Downlaod to local job dir
        path = path[:-1] if path[-1]=='/' else path
        fname = '/'.join(path.split('/')[-1:])
        job_folder = '/'.join(path.split('/')[:-1])

        # Make sure job file/folder exists
        files = self.ls_job(jobId, path=job_folder)
        if fname not in files:
            msg = f"Unable to find job file {path} for job {jobId}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        # Get data
        src_path = '/'.join([self.jobs_dir, jobId, path])
        try:
            data = self.get_data(src_path, data_type=data_type)
        except Exception as e:
            msg = f"Unable to get data of type {data_type} from job file {src_path}."
            logger.error(msg)
            raise Exception(msg)
        return data


    def upload_job_data(self, job_id, path, dest_dir='.', file_filter='*'):
        """
        Upload file/folder at local `path` to `dest_dir`, relative to job directory on remote TACC
        system.

        Parameters
        ----------
        jobId : str
            ID of job.
        path: str
            Path to local file or folder to send to job directory on TACC system.
        dest_dir: str
            Destination unix-style path, relative to `job_id`'s job directory, of file/folder.
            If a file is being sent, remote is the destination path. If a folder is
            being sent, remote is the folder where the file contents will go.
            Note that if path not absolute, then it's relative to user's home directory.
        file_filter: str, optional, Default = '*'
            If a folder is being uploaded, unix style pattern matching string to use on
            files to upload within folder. For example, '*.txt' would only upload .txt files.

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
        Will overwrite existing files and folders and is recursive for folders being sent.
        If job is currently executing, then this could disrupt job behavior, be careful!
        Remote paths must use unix path seperator '/' since all TACC systems are unix.
        """
        try:
            # Get destination directory in job path to send file to
            fname = os.path.basename(os.path.normpath(path))
            dest_path = '/'.join([self.jobs_dir, job_id, dest_dir, fname])

            self.upload(path, dest_path, file_filter=file_filter)
        except Exception as e:
            msg = f"Unable to send file {path} to destination destination {dest_path}"
            logger.error(msg)
            raise Exception(msg)
        return dest_path


    def send_job_data(self, job_id, data, path):
        """
        Send `data` to `path`, relative to `job_id`'s job directory on remote TACC system,
        via an sftp file stream. Supported types for `data` are:
            1. dict -> json file
            2. text -> text file

        Parameters
        ----------
        jobId : str
            ID of job.
        data : dict or str
            Dictionary of data to send and save as a json file if dict or text
            data to be saved as text file if str.
        path : str
            Unix-style path relative to `job_id`'s job directory on TACC system to save data to.

        Returns
        -------
        None

        Warnings
        --------
        Will overwrite existing files. If job is currently executing, then this could disrupt
        job behavior, be careful! Remote paths must use unix path seperator '/' since all TACC
        systems are unix.
        """
        try:
            # Get destination directory in job path to send file to
            dest_path = '/'.join([self.jobs_dir, job_id, path])
            self.send_data(data, dest_path)
        except Exception as e:
            msg = f"Unable to send file {path} to destination destination {dest_path}"
            logger.error(msg)
            raise Exception(msg)


    def peak_job_file(self, job_id, fpath, head=-1, tail=-1):
        # Load job config
        path = '/'.join([self.jobs_dir, job_id, fpath])

        return self.peak_file(path, head=head, tail=tail)


    def deploy_script(self, script_name, local_file=None):
        """
        Deploy a script to TACC

        Parameters
        ----------
        script_name : str
            The name of the script. Will be used as the local filename unless
            local_file is passed. If the filename ends in .py, it will be
            assumed to be a Python3 script. Otherwise, it will be treated as a
            generic executable.
        local_file : str
            The local filename of the script if not passed, will be inferred
            from script_name.
        Returns
        -------
        """

        local_fname = local_file if local_file is not None else script_name
        if not os.path.exists(local_fname):
            raise ValueError(f"Could not find script file - {local_fname}!")

        # Extract basename in case the script_name is a path
        script_name = script_name.split("/")[-1]
        if "." in script_name:
            # Get the extension, and be robust to mulitple periods in the filename just in case.
            parts = script_name.split(".")
            script_name, ext = ".".join(parts[:-1]), parts[-1]

        remote_path = self.scripts_dir+"/"+script_name
        if ext == "py":
            # assume Python3
            python_path = self._execute_command("module load python3 > /dev/null; which python3")
            with open(local_fname, 'r') as fp:
                script = fp.read()

            with self._client.open_sftp() as sftp:
                with sftp.open(remote_path, "w") as fp:
                    fp.write("#!" + python_path + "\n" + script)
        else:
            self.upload(local_fname, remote_path)

        self._execute_command(f"chmod +x {remote_path}")


    def run_script(self, script_name, job_id=None, args=None):
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
        if args is None: args = []
        if job_id is not None: args.insert(0, '/'.join([self.jobs_dir, job_id]))

        return self._execute_command(f"{self.scripts_dir}/{script_name} {' '.join(args)}")

    def list_scripts(self):
        """
        List scripts deployed in this TACCJobManager Instance

        Parameters
        ----------

        Returns
        -------
        scripts : list of str
            List of scripts in TACC Job Manager's scripts directory.
        """
        return self.list_files(self.scripts_dir)
