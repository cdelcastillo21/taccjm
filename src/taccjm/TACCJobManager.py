"""
TACCJobManager Class


"""

import os  # OS system utility functions for local system
import posixpath  # Paths on remote system (assumed UNIX)
import errno  # For error messages
import tarfile  # For sending compressed directories
import re  # Regular Expressions
import pdb  # Debug
import tempfile
import json  # For saving and loading job configs to disk
import time  # Time functions
import logging  # Used to setup the Paramiko log file
import stat  # For reading file stat codes
from datetime import datetime  # Date time functionality
from fnmatch import fnmatch  # For unix-style filename pattern matching
from taccjm.utils import *  # TACCJM Util functions for config files/dicts
from taccjm.constants import *  # For application configs
from typing import Tuple, List  # Type hints
from typing import Union  # Type hints
from pandas import to_datetime

# Modified paramiko ssh client and common paramiko exceptions
from taccjm.SSHClient2FA import SSHClient2FA
from paramiko import SSHException, AuthenticationException, BadHostKeyException

# Custom exception for handling remote command errors
from taccjm.exceptions import TJMCommandError

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


logger = logging.getLogger(__name__)


class TACCJobManager:
    """
    Class defining an ssh connection to a TACC system .

    Attributes
    ----------
    system : str
        Name of remote system to connect to. Supported systems: stampede2,
        ls5, frontera, maverick2
    user : str
        Name of tacc user connecting via ssh to resource.
    jobs_dir : str
        Unix-stlye path to directory to store jobs.
    apps_dir : str
        Unix-style path to directory to store applications.
    scripts_dir : str
        Unix-stye path to directory where scripts for job manager can be found.
    trash_dir : str
        Unix-stye path to directory where unwanted files/folders go.

    Methods
    -------
    showq
    get_allocations
    list_files
    peak_file
    upload
    download
    remove
    restore
    write
    read
    get_apps
    get_app
    deploy_app
    get_jobs
    get_job
    deploy_job
    submit_job
    cancel_job
    remove_job
    download_job_file
    read_job_file
    upload_job_file
    write_job_file
    deploy_script
    run_script
    list_scripts

    Notes
    -----
    All remote paths should use Unix path seperateor '/'.

    """

    ROOT = "tacc.utexas.edu"
    SYSTEMS = ["stampede2", "ls6", "frontera", "maverick2"]
    USER_PROMPT = "Username:"
    PSW_PROMPT = "Password:"
    MFA_PROMPT = "TACC Token Code:"
    SCRATCH_DIR = "$SCRATCH"

    def __init__(self, system, user=None, psw=None, mfa=None, working_dir="taccjm"):
        """
        Initialize Job Manager connection and directories.

        Parameters
        ----------
        system : str
            Name of system to connect to via ssh. Supported systems:
            stampede2, ls5, frontera, maverick2
        user : str , optional
            Name of user connecting. If non provided input prompt will appear.
        psw : str , optional
            Password for user connecting. Secure prompt will appear if no
            password is provided.
        mfa : str , optional
            2-Factor Authenticaion token for user.. If non provided
            input prompt will appear
        working_dir: str , default='taccjm'
            Unix-style path relative to user's SCRATCH directory to place all
            data related to this job manager instance. This includes the apps,
            jobs, scripts, and trash directories.
        """

        if system not in self.SYSTEMS:
            m = f"Unrecognized system {system}. Options - {self.SYSTEMS}."
            logger.error(m)
            raise ValueError(m)
        if any(
            [
                working_dir.startswith("../"),
                working_dir.endswith("/.."),
                "/../" in working_dir,
            ]
        ):
            msg = f"Inavlid working dir {working_dir} - Contains '..'."
            logger.error(msg)
            raise ValueError(msg)

        # Connect to system
        logger.info(f"Connecting {user} to system {system}...")
        self.system = f"{system}.tacc.utexas.edu"
        self._client = SSHClient2FA(
            user_prompt=self.USER_PROMPT,
            psw_prompt=self.PSW_PROMPT,
            mfa_prompt=self.MFA_PROMPT,
        )
        self.user = self._client.connect(self.system, uid=user, pswd=psw, mfa_pswd=mfa)
        logger.info(f"Succesfuly connected to {system}")

        # Get taccjm working directory, relative to users scratch directory
        self.SCRATCH_DIR = scratch_dir = self._execute_command(
            f"echo {self.SCRATCH_DIR}"
        ).strip()
        logger.info(
            "Resolved scratch path  to ${self.SCRATCH_DIR} for user {self.user}"
        )
        taccjm_dir = posixpath.join(self.SCRATCH_DIR, working_dir)

        # Initialze jobs, apps, scripts, and trash dirs
        logger.info("Creating if jobs, apps, scripts, and trash dirs")
        for d in zip(
            ["jobs_dir", "apps_dir", "scripts_dir", "trash_dir"],
            ["jobs", "apps", "scripts", "trash"],
        ):
            setattr(self, d[0], posixpath.join(taccjm_dir, d[1]))
            logger.info(f"Initializing directory {getattr(self, d[0])}")
            self._mkdir(getattr(self, d[0]), parents=True)

        # Get python path and home dir
        self.python_path = self._execute_command("which python")

        self.scripts = []

    def _execute_command(self, cmnd, wait=True) -> None:
        """
        Executes a shell command through ssh on a TACC resource.

        Parameters
        ----------
        cmnd : str
            Command to execute. Be careful! rm commands and such will delete
            things permenantly!

        Returns
        -------
        out : str
            stdout return from command.

        Raises
        ------
        TJMCommandError
            If command executed on TACC resource returns non-zero return code.
        """

        transport = self._client.get_transport()
        try:
            channel = transport.open_session()
        except SSHException as ssh_error:
            # Will only occur if ssh connection is broken
            msg = "TACCJM ssh connection error: {ssh_error.__str__()}"
            logger.error(msg)
            raise ssh_error

        channel.exec_command(cmnd)
        if wait:
            rc, out, error = self._process_command(cmnd, channel)
            return out
        else:
            return channel

    def _process_command(self, cmnd, channel, error=True):
        """
        Process command

        Wait until a command finishes and read stdout and stderr. Raise error if
        anything in stderr, else return stdout.
        """
        rc = channel.recv_exit_status()
        out = channel.recv(1000000).decode("utf-8")
        err = channel.recv_stderr(1000000).decode("utf-8")

        if rc != 0 and error:
            # Build base TJMCommand Error, only place this should be done
            t = TJMCommandError(self.system, self.user, cmnd, rc, out, err)

            # Only log the actual TJMCommandError object once, here
            logger.error(t.__str__())

            raise t

        return (rc, out, err)

    def _mkdir(self, path, parents=False) -> None:
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
            tjm_error.message = f"_mkdir - Could not create directory {path}"
            logger.error(tjm_error.message)
            raise tjm_error

    def _parse_submit_script(self, job_config: dict) -> str:
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
        # Format submit scripts with appropriate inputs for job
        np = job_config["node_count"] * job_config["processors_per_node"]
        header = SUBMIT_SCRIPT_TEMPLATE.format(
            name=job_config["name"],
            desc=job_config["desc"],
            job_id=job_config["job_id"],
            queue=job_config["queue"],
            N=job_config["node_count"],
            n=np,
            rt=job_config["max_run_time"],
        )

        # Add slurm directives for email and allocation if specified for job
        if "email" in job_config.keys():
            email_line = "\n#SBATCH --mail-user={email} # Email to send to"
            header += email_line.format(email=job_config["email"])
            header += "\n#SBATCH --mail-type=all     # Email to send to"
        if "allocation" in job_config.keys():
            alloc_line = "\n#SBATCH -A {allocation} # Allocation name "
            header += alloc_line.format(allocation=job_config["allocation"])

        # End slurm directives
        header += "\n#----------------------------------------------------\n"

        # Build function arguments
        # always pass total number of MPI processes
        job_args = {"NP": np}

        # Add paths of job inputs (transferred to job dir) as argument
        for arg, path in job_config["inputs"].items():
            job_args[arg] = "/".join(
                [job_config["job_dir"], "inputs", os.path.basename(path)]
            )

        # Add on parameters passed to job
        job_args.update(job_config["parameters"])

        # Create list of arguments to pass as env variables to job
        export_list = [f"export TACCJM_APPID={job_config['app']}"]
        for arg, value in job_args.items():
            # wrap with single quotes if needed
            value = str(value)
            needs_quote = " " in value and not (value[0] == value[-1] == "'")
            value = f"'{value}'" if needs_quote else value
            export_list.append(f"export {arg}={value}")

        # Parse final submit script
        entry_path = f"{job_config['job_dir']}/{job_config['app']}"
        entry_path += f"/{job_config['entry_script']}"
        submit_script = (
            header  # set SBATCH params
            + f"\ncd {job_config['job_dir']}\n\n"  # cd to job directory
            + "\n".join(export_list)  # set job params
            + f"\n{entry_path}"  # run main script
        )

        return submit_script

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
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "showq - TACC SLURM queue is not accessible."
            logger.error(tjm_error.message)
            raise tjm_error

        # Loop through lines in output table and parse job information
        jobs = []
        parse = lambda x: {
            "job_id": x[0],
            "job_name": x[1],
            "username": x[2],
            "state": x[3],
            "nodes": x[4],
            "remaining": x[4],
            "start_time": x[5],
        }
        lines = ret.split("\n")
        jobs_line = False
        line_counter = -2
        for l in lines:
            job_statuses = ["ACTIVE", "WAITING", "BLOCKED", "COMPLETING"]
            if any([l.startswith(x) for x in job_statuses]):
                jobs_line = True
                continue
            if jobs_line == True:
                if l == "":
                    jobs_line = False
                    line_counter = -2
                    continue
                else:
                    line_counter += 1
                    if line_counter > 0:
                        jobs.append(parse(l.split()))

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
            ret = self._execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "get_allocations - Unable to get allocation info"
            logger.error(tjm_error.message)
            raise tjm_error

        # Parse allocation info
        allocations = set([x.strip() for x in ret.split("\n")[2].split("|")])
        allocations.remove("")
        allocations = [x.split() for x in allocations]
        allocations = [
            {"name": x[0], "service_units": int(x[1]), "exp_date": x[2]}
            for x in allocations
        ]

        return allocations

    def list_files(self, path: str = ".") -> List[dict]:
        """
        Returns the info on all files/folderes at a given path. If path is a
        file, then returns file info. If path is directory, then returns file
        info on all contents in directory.

        Parameters
        ----------
        path : str, default='.'
            Path, relative to user's home directory, to get file(s) info.

        Returns
        -------
        files : list of dict
            List of dictionaries containing file info including:
                - filename : Filename
                - st_atime : Last accessed time
                - st_gid   : Group ID
                - st_mode  : Type/Permission bits of file. Use stat library.
                - st_mtime : Last modified time.
                - st_size  : Size in bytes
                - st_uid   : UID of owner.
                - asbytes  : Output from an ls -lat like command on file.

        Raises
        ------
        TJMCommandError
            If can't access path for any reason on TACC system. This may be
            because the TACC user doesn't have permissions to view the given
            directory or that the path does not exist, for exmaple.

        """
        try:
            f_info = []
            f_attrs = ["st_atime", "st_gid", "st_mode", "st_mtime", "st_size", "st_uid"]

            # Open sftp connection
            with self._client.open_sftp() as sftp:
                # Query path to see if its directory or file
                lstat = sftp.lstat(path)

                if stat.S_ISDIR(lstat.st_mode):
                    # If directory get info on all files in directory
                    f_attrs.insert(0, "filename")
                    files = sftp.listdir_attr(path)
                    for f in files:
                        # Extract fields from SFTPAttributes object for files
                        d = dict([(x, f.__getattribute__(x)) for x in f_attrs])
                        d["ls_str"] = f.asbytes()
                        f_info.append(d)
                else:
                    # If file, just get file info
                    d = [(x, lstat.__getattribute__(x)) for x in f_attrs]
                    d.insert(0, ("filename", path))
                    d.append(("ls_str", lstat.asbytes()))
                    f_info.append(dict(d))

            # Return list of dictionaries with file info
            return f_info
        except FileNotFoundError as f:
            msg = f"list_files - No such file or folder"
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, path)
        except PermissionError as p:
            msg = f"list_files - Permission denied"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, path)
        except SSHException as s:
            msg = f"list_files - Session is stale. Restart job manager."
            logger.error(msg)
            raise s
        except Exception as e:
            msg = f"list_files - Unknown error trying to access {path}: {e}"
            logger.error(msg)
            raise e

    def peak_file(self, path: str, head: int = -1, tail: int = -1) -> str:
        """
        Performs head/tail on file at given path to "peak" at file.

        Parameters
        ----------
        path : str
            Unix-style path, relative to users home dir, of file to peak at.
        head : int, optional, default=-1
            If greater than 0, the number of lines to get from top of file.
        tail : int, optional, default=-1
            If greater than 0, the number of lines to get from bottom of file.
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
        """
        if head > 0:
            cmnd = f"head -{head} {path}"
        elif tail > 0:
            cmnd = f"tail -{tail} {path}"
        else:
            cmnd = f"head {path}"
        try:
            ret = self._execute_command(cmnd)
        except TJMCommandError as t:
            f_not_found = ["No such file or directory", "Not a directory"]
            if "Permission denied" in t.stderr:
                msg = f"peak_file - Dont have permission to access {path}"
                raise PermissionError(errno.EACCES, msg, path)
            elif any([x in t.stderr or x in t.stdout for x in f_not_found]):
                msg = f"peak_file - No such file or directory {path}"
                logger.error(msg)
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
            else:
                t.message = f"peak_file - Unexpected error."
                logger.error(t.message)
                raise t

        return ret

    def upload(self, local: str, remote: str, file_filter: str = "*") -> None:
        """
        Sends file or folder from local path to remote path. If a file is
        specified, the remote path is the destination path of the file to be
        sent. If a folder is specified, all folder contents (recursive) are
        compressed into a .tar.gz file before being sent and then the contents
        are unpacked in the specified remote path.

        Parameters
        ----------
        local : str
            Path to local file or folder to send to TACC system.
        remote : str
            Destination unix-style path for the file/folder being sent on the
            TACC system. If a file is being sent, remote is the destination
            path. If a folder is being sent, remote is the folder where the
            file contents will go. Note that if path not absolute, then it's
            relative to user's home directory.
        file_filter: str, optional, Default = '*'
            If a folder is being uploaded, unix style pattern matching string
            to use on files to download. For example, '*.txt' would only
            download .txt files.

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
        Will overwrite existing files and folders and is recursive for folders
        being sent. Remote paths must use unix path seperator '/' since all
        TACC systems are unix.

        """
        # Unix paths -> Get file remote file name and directory
        remote_dir, remote_fname = os.path.split(remote)
        remote_dir = "." if remote_dir == "" else remote_dir

        try:
            # Sending directory -> Package into tar file
            if os.path.isdir(local):
                fname = os.path.basename(local)
                local_tar_file = f".{fname}.taccjm.tar"
                remote_tar_file = f"{remote_dir}/.taccjm_temp_{fname}.tar"

                # Package tar file -> Recursive call
                with tarfile.open(local_tar_file, "w:gz") as tar:
                    f = lambda x: x if fnmatch(x.name, file_filter) else None
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
                # If tar command fails, the remove command should still work
                untar_cmd = f"tar -xzvf {remote_tar_file} -C {remote_dir}; "
                untar_cmd += f"rm -rf {remote_tar_file}"
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
            t.message = f"upload - Error unpacking tar file"
            logger.error(t.message)
            raise t
        except Exception as e:
            msg = f"upload - Unexpected error {e.__str__()}"
            logger.error(msg)
            raise e

    def download(self, remote: str, local: str, file_filter: str = "*") -> None:
        """
        Downloads file or folder from remote path on TACC resource to local
        path. If a file is specified, the local path is the destination path
        of the file to be downloaded If a folder is specified, all folder
        contents (recursive) are compressed into a .tar.gz file before being
        downloaded and the contents are unpacked in the specified local
        directory.

        Parameters
        ----------
        remote : str
            Unix-style path to file or folder on TACC system to download.
            Note that path is relative to user's home directory.
        local : str
            Destination where file/folder being downloaded will be placed.
            If a file is being downloaded, then local is the destination path.
            If a folder is being downloaded, local is where folder downloaded
            will be placed
        file_filter: str, optional, default='*'
            If a folder is being download, unix style pattern matching string
            to use on files to download. For example, '*.txt' would only
            download .txt files.

        Returns
        -------

        Raises
        ------
        FileNotFoundError
            If local or remote file/folder do not exist
        PermissionError
            If user does not have permission to write to specified remote path
            on TACC system or access to local file/folder and contents.
        TJMCommandError
            If a directory is being downloaded, this error is thrown if there
            are any issues packing the .tar.gz file on the remote system before
            downloading.

        Warnings
        --------
        Will overwrite existing files and folders and is recursive for folders
        being downloaded. Remote paths must use unix path seperator '/' since
        all TACC systems are unix.
        """

        local = os.path.abspath(local.rstrip("/"))
        remote = remote.rstrip("/")
        try:
            with self._client.open_sftp() as sftp:
                fileattr = sftp.stat(remote)
                is_dir = stat.S_ISDIR(fileattr.st_mode)
                if is_dir:
                    dirname, fname = posixpath.split(remote)
                    remote_tar = f"{dirname}/{fname}.tar.gz"

                    # Build command. Filter files according to file filter
                    cmd = f"cd {dirname} && "
                    cmd += f"find {fname} -name '{file_filter}' -print0 | "
                    cmd += f"tar -czvf {remote_tar} --null --files-from -"

                    # Try packing remote tar file
                    try:
                        self._execute_command(cmd)
                    except TJMCommandError as t:
                        if "padding with zeros" in t.stdout:
                            # Warning message, not an error.
                            pass
                        else:
                            # Other error tar-ing file. Raise
                            raise t

                    # try to get tar file
                    try:
                        local_tar = f"{local}.tar.gz"
                        sftp.get(remote_tar, local_tar)
                    except Exception as e:
                        os.remove(local_tar)
                        raise e

                    # unpack tar file locally
                    with tarfile.open(local_tar) as tar:
                        tar.extractall(path=local)

                    # Remove local and remote tar files
                    os.remove(local_tar)
                    self._execute_command(f"rm -rf {remote_tar}")
                else:
                    # Get remote file
                    sftp.get(remote, local)
        except FileNotFoundError as f:
            msg = f"download - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"download - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except TJMCommandError as t:
            msg = f"download - Error tar-ing remote file."
            t.message = msg
            logger.error(msg)
            raise t

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
        file_name = remote_path.replace("/", "___")
        trash_path = f"{self.trash_dir}/{file_name}"
        abs_path = "./" + remote_path if remote_path[0] != "/" else remote_path
        abs_path = posixpath.normpath(abs_path)

        # Check if path is a file or directory
        is_dir = False
        try:
            with self._client.open_sftp() as sftp:
                fileattr = sftp.stat(abs_path)
                is_dir = stat.S_ISDIR(fileattr.st_mode)
        except FileNotFoundError as f:
            msg = f"remove - No such file/folder {abs_path}"
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, abs_path)

        src_path = abs_path if not is_dir else abs_path + "/"
        cmnd = f"rsync -a {src_path} {trash_path} && rm -rf {abs_path}"
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

    def write(self, data: Union[str, dict], path: str) -> None:
        """
        Write `data` directly to path via an sftp file stream. Supported types:
            1. dict -> json file
            2. text -> text file

        Parameters
        ----------
        data : dict or str
            Dictionary of data to write as a json file or text data be written
            as text file.
        path : str
            Unix-style path on TACC system to save data to.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.
        PermissionError
            If user does not have permission to write to specified remote path.
        """
        d_type = type(data)
        if d_type not in [dict, str]:
            raise ValueError(f"Data type {d_type} is not supported")
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(path, "w") as jc:
                    if d_type == dict:
                        json.dump(data, jc)
                    else:
                        jc.write(data)
        except FileNotFoundError as f:
            msg = f"write - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"write - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except Exception as e:
            msg = f"write - Unknown error writing data to {path}: {e}"
            logger.error(msg)
            raise e

    def read(self, path: str, data_type: str = "text") -> Union[str, dict]:
        """
        Read data of `data_type` from file `path` on TACC system directly via
        a file stream. Supported data types are:
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
        data : str or dict
            Either text or dictionary containing data stored on remote system.

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.
        PermissionError
            If user does not have permission to read specified remote path
        """
        if data_type not in ["json", "text"]:
            raise ValueError(f"read - data type {data_type} is not supported")
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(path, "r") as fp:
                    if data_type == "json":
                        data = json.load(fp)
                    else:
                        data = fp.read().decode("UTF-8")
            return data
        except FileNotFoundError as f:
            msg = f"read - No such file or folder {f.filename}."
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"read - Permission denied on {p.filename}"
            logger.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except Exception as e:
            msg = f"read - Unknown reading data from {path}: {e}"
            logger.error(msg)
            raise e

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
        apps = [f["filename"] for f in self.list_files(path=self.apps_dir)]
        apps = [a for a in apps if not a.startswith(".")]
        return apps

    def get_app(self, app_id: str) -> dict:
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
            logger.error(msg)
            raise ValueError(msg)

        # Load application config
        app_config_path = "/".join([self.apps_dir, app_id, "app.json"])
        app_config = self.read(app_config_path, data_type="json")

        return app_config

    def deploy_app(
        self,
        app_config: dict = None,
        local_app_dir: str = ".",
        app_config_file: str = "app.json",
        overwrite: bool = False,
        **kwargs,
    ) -> dict:
        """
        Deploy local application to TACCJobManager.apps_dir. Values in project
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

        # Load templated app configuration
        if app_config is None:
            with open(os.path.join(local_app_dir, app_config_file), "r") as fp:
                app_config = json.load(fp)

        # Update with kwargs
        app_config.update(**kwargs)

        # Get current apps already deployed
        cur_apps = self.get_apps()

        # Required parameters for application configuration
        missing = set(APP_TEMPLATE.keys()) - set(app_config.keys())
        if len(missing) > 0:
            msg = f"deploy_app - missing required app configs {missing}"
            logger.error(msg)
            raise ValueError(msg)

        # Only overwrite previous version of app if overwrite is set.
        if (app_config["name"] in cur_apps) and (not overwrite):
            msg = f"deploy_app - {app_config['name']} already exists."
            logger.info(msg)
            raise ValueError(msg)

        try:
            # Now try and send application data and config to system
            local_app_dir = os.path.join(local_app_dir, "assets")
            remote_app_dir = "/".join([self.apps_dir, app_config["name"]])
            self.upload(local_app_dir, remote_app_dir)

            # Put app config in deployed app folder
            app_config_path = "/".join([remote_app_dir, "app.json"])
            self.write(app_config, app_config_path)

            # Make entry point script executable
            entry_script = f"{remote_app_dir}/{app_config['entry_script']}"
            self._execute_command(f"chmod +x {entry_script}")

        except Exception as e:
            msg = f"deploy_app - Unable to stage appplication data."
            logger.error(msg)
            raise e

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
        jobs = [f["filename"] for f in self.list_files(path=self.jobs_dir)]
        jobs = [j for j in jobs if not j.startswith(".")]
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
        try:
            job_config_path = "/".join([self.jobs_dir, job_id, "job.json"])
            return self.read(job_config_path, data_type="json")
        except FileNotFoundError as e:
            # Invalid job ID because job doesn't exist
            msg = f"get_job - {job_id} does not exist."
            logger.error(msg)
            raise ValueError(msg)

    def deploy_job(
        self,
        job_config: dict = None,
        local_job_dir: str = ".",
        job_config_file: str = "job.json",
        stage: bool = True,
        **kwargs,
    ) -> dict:
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
            with open(os.path.join(local_job_dir, job_config_file), "r") as fp:
                job_config = json.load(fp)

        # Overwrite job_config loaded with kwargs keyword arguments if specified
        job_config = update_dic_keys(job_config, **kwargs)

        # Get default arguments from deployed application
        app_config = self.get_app(job_config["app"])

        def _get_attr(j, a):
            # Helper function to get app defatults for job configs
            if j in job_config.keys():
                return job_config[j]
            else:
                return app_config[a]

        # Default in job arguments if they are not specified
        job_config["entry_script"] = _get_attr("entry_script", "entry_script")
        job_config["desc"] = _get_attr("desc", "short_desc")
        job_config["queue"] = _get_attr("queue", "default_queue")
        job_config["node_count"] = int(_get_attr("node_count", "default_node_count"))
        job_config["processors_per_node"] = int(
            _get_attr("processors_per_node", "default_processors_per_node")
        )
        job_config["max_run_time"] = _get_attr("max_run_time", "default_max_run_time")

        # Verify appropriate inputs and arguments are passed
        for i in app_config["inputs"]:
            if i["name"] not in job_config["inputs"]:
                raise ValueError(f"deploy_job - missing input {i['name']}")
        for i in app_config["parameters"]:
            if i["name"] not in job_config["parameters"]:
                raise ValueError(f"deploy_job - missing parameter {i['name']}")

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
        ret = self._execute_command(cmnd)
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
        except:
            pass

        # Remove job directory, if it still exists
        job_dir = "/".join([self.jobs_dir, job_id])
        try:
            self.remove(job_dir)
        except:
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

    def ls_job(self, job_id: str, path: str = "") -> List[dict]:
        """
        Get info on files in job directory.

        Parameters
        ----------
        job_id : str
            ID of job.
        path: str, default=''
            Directory, relative to the job directory, to query.


        Returns
        -------
        files : list of dict
            List of dictionaries containing file info including:
                - filename : Filename
                - st_atime : Last accessed time
                - st_gid   : Group ID
                - st_mode  : Type/Permission bits of file. Use stat library.
                - st_mtime : Last modified time.
                - st_size  : Size in bytes
                - st_uid   : UID of owner.
                - asbytes  : Output from an ls -lat like command on file.
        """
        # Get files from particular directory in job
        fpath = "/".join([self.jobs_dir, job_id, path])
        files = self.list_files(path=fpath)

        return files

    def download_job_file(
        self, job_id: str, path: str, dest_dir: str = ".", file_filter: str = "*"
    ) -> str:
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
        path = path[:-1] if path[-1] == "/" else path
        fname = "/".join(path.split("/")[-1:])
        job_folder = "/".join(path.split("/")[:-1])

        # Make local data directory if it doesn't exist already
        local_data_dir = os.path.join(dest_dir, job_id)
        os.makedirs(local_data_dir, exist_ok=True)

        # Get file
        src_path = "/".join([self.jobs_dir, job_id, path])
        dest_path = os.path.join(local_data_dir, fname)
        try:
            self.download(src_path, dest_path, file_filter=file_filter)
        except Exception as e:
            m = f"download_job_file - Unable to download {src_path}"
            logger.error(m)
            raise e
        return dest_path

    def read_job_file(
        self, job_id: str, path: str, data_type: str = "text"
    ) -> Union[str, dict]:
        """
        Read data of `data_type` from file `path` relative to a job's directory
        via an sftp file stream. Supported data types are:
            1. text -> str (Default)
            2. json -> dict

        Parameters
        ----------
        job_id : str
            ID of job.
        path : str
            Unix-style path, relative to job directory on TACC system,
            containing desired data.
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
        path = path[:-1] if path[-1] == "/" else path
        fname = "/".join(path.split("/")[-1:])
        job_folder = "/".join(path.split("/")[:-1])

        # Get data
        src_path = "/".join([self.jobs_dir, job_id, path])
        try:
            data = self.read(src_path, data_type=data_type)
        except Exception as e:
            msg = f"read_job_file - Unable read {data_type} from {src_path}."
            logger.error(msg)
            raise e

        return data

    def upload_job_file(
        self, job_id: str, path: str, dest_dir: str = ".", file_filter: str = "*"
    ) -> None:
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
            dest_path = "/".join([self.jobs_dir, job_id, dest_dir, fname])

            self.upload(path, dest_path, file_filter=file_filter)
        except Exception as e:
            msg = f"upload_job_file - Unable to upload {path} to {dest_path}."
            logger.error(msg)
            raise e
        return dest_path

    def write_job_file(self, job_id: str, data: Union[dict, str], path: str) -> None:
        """
        Write `data` to `path`, relative to `job_id`'s job directory on remote
        TACC system, via an sftp file stream. Supported types for `data` are:
            1. dict -> json file
            2. text -> text file

        Parameters
        ----------
        job_id : str
            ID of job.
        data : dict or str
            Dictionary of data to send and save as a json file if dict or text
            data to be saved as text file if str.
        path : str
            Unix-style path relative to `job_id`'s job directory on TACC
            system to save data to.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If local file/folder does not exist, or remote destination path
            is invalid does not exist.
        PermissionError
            If user does not have permission to write to specified remote path.

        Warnings
        --------
        Will overwrite existing files. If job is currently executing, then this
        could disrupt job behavior, be careful! Remote paths must use unix path
        seperator '/' since all TACC systems are unix.
        """
        try:
            # Get destination directory in job path to send file to
            dest_path = "/".join([self.jobs_dir, job_id, path])
            self.write(data, dest_path)
        except Exception as e:
            msg = f"write_job_file - Unable to write file {path}."
            logger.error(msg)
            raise e

    def peak_job_file(
        self, job_id: str, path: str, head: int = -1, tail: int = -1
    ) -> str:
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
        path = "/".join([self.jobs_dir, job_id, path])

        try:
            return self.peak_file(path, head=head, tail=tail)
        except Exception as e:
            msg = f"peak_job_file - Unable to peak at file {path}."
            logger.error(msg)
            raise e

    def deploy_script(self, script_name: str, local_file: str = None) -> None:
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
        script_name, ext = os.path.splitext(os.path.basename(script_name))
        remote_path = posixpath.join(self.scripts_dir, script_name)

        # If python file, add directive to python3 path on TACC system
        if ext == ".py":
            script = "#!" + self.python_path + "\n"
            with open(local_fname, "r") as fp:
                script += fp.read()
            self.write(script, remote_path)
        else:
            self.upload(local_fname, remote_path)

        # Make remote script executable
        self._execute_command(f"chmod +x {remote_path}")

    def run_script(
        self,
        script_name: str,
        job_id: str = None,
        args: List[str] = [],
        wait: bool = False,
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

        if wait:
            return self._execute_command(run_cmd)
        else:
            channel = self._execute_command(run_cmd, wait=False)
            script_id = len(self.scripts) + 1
            script_config = {
                "id": script_id,
                "name": script_name,
                "ts": ts,
                "status": "STARTED",
                "cmd": run_cmd,
                "path": f"{self.scripts_dir}/{script_name}",
                "channel": channel,
                "stdout": "",
                "stderr": "",
                "logfile": logfile,
                "errfile": errfile,
                "history": [],
            }
            self.scripts.append(script_config)
            return script_config

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
        sc = [f["filename"] for f in self.list_files(path=self.scripts_dir)]
        return sc

    def get_script_status(self, script_id, nbytes: int = None):
        """
        Poll a running script for its status.

        """
        if script_id <= len(self.scripts):
            script_config = self.scripts[script_id - 1]
            if script_config["status"] not in ["COMPLETE", "FAILED"]:
                script_config["history"].append(
                    {"ts": script_config["ts"], "stats": script_config["status"]}
                )
                if script_config["channel"].exit_status_ready():
                    rc, out, error = self._process_command(
                        script_config["cmd"], script_config["channel"], error=False
                    )
                    ts = datetime.fromtimestamp(time.time()).strftime("%Y%m%d_%H%M%S")
                    script_config["ts"] = ts
                    script_config["status"] = "FAILED" if rc != 0 else "COMPLETE"
                    script_config["stdout"] += out
                    script_config["stderr"] += error
                    script_config["rc"] = rc
                    script_config["rt"] = (
                        to_datetime(" ".join(ts.split("_")))
                        - to_datetime(
                            " ".join(script_config["history"][0]["ts"].split("_"))
                        )
                    ).seconds
                    script_config["channel"].close()
                else:
                    if nbytes is not None:
                        script_config["stdout"] += (
                            script_config["channel"].recv(nbytes).decode("utf-8")
                        )
                    script_config["ts"] = datetime.fromtimestamp(time.time()).strftime(
                        "%Y%m%d_%H%M%S"
                    )
                    script_config["status"] = "RUNNING"
                self.scripts[script_id - 1] = script_config
        else:
            raise ValueError(f"Invalid script_id {script_id}.")

        ret = {f"{i}": script_config[i] for i in script_config}
        return ret

    def run_script_str(
        self,
        name: str,
        cmnd: str,
        job_id: str = None,
        args: List[str] = [],
        logfile: bool = False,
        errfile: bool = True,
    ) -> str:
        """
        Run str command as script
        """
        res = None
        temp_file = tempfile.NamedTemporaryFile(delete=True)
        temp_file.write(f"#!/bin/bash\n{cmnd}".encode("utf-8"))
        temp_file.flush()
        self.deploy_script(name, temp_file.name)
        temp_file.close()
        res = self.run_script(
            script_name=name,
            job_id=job_id,
            args=args,
            wait=False,
            logfile=logfile,
            errfile=errfile,
        )

        return res
