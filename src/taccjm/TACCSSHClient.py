"""
TACCSSHClient Class

A class that defines an ssh connection to a TACC system.


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
from taccjm.utils import init_logger, tar_file
from taccjm.constants import *  # For application configs
from typing import Tuple, List  # Type hints
from typing import Union  # Type hints
from pandas import to_datetime

# Modified paramiko ssh client and common paramiko exceptions
from taccjm.SSHClient2FA import SSHClient2FA
from paramiko import SSHException, AuthenticationException, BadHostKeyException

# Custom exception for handling remote command errors
from taccjm.exceptions import TJMCommandError, SSHCommandError

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


class TACCSSHClient(SSHClient2FA):
    """
    Class defining an ssh connection to a TACC system .

    Attributes
    ----------
    system : str
        Name of remote system to connect to. Supported systems: stampede2,
        ls5, frontera, maverick2
    user : str
        Name of tacc user connecting via ssh to resource.
    trash_dir : str
        Unix-stye path to directory where unwanted files/folders go.

    Methods
    -------
    execute_command
    process_command
    list_files
    upload_file
    upload_dir
    upload
    download
    remove
    restore
    write
    read

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

    def __init__(
        self,
        system,
        user=None,
        psw=None,
        mfa=None,
        working_dir="taccjm",
        log=None,
        logfmt="txt",
        loglevel=logging.ERROR,
    ):
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

        # Initialize parent class with appropriate prompts for login to TACC systems
        super().__init__(
            user_prompt=self.USER_PROMPT,
            psw_prompt=self.PSW_PROMPT,
            mfa_prompt=self.MFA_PROMPT,
        )

        self.log = init_logger(__name__, output=log, fmt=logfmt, loglevel=loglevel)

        if system not in self.SYSTEMS:
            m = f"Unrecognized system {system}. Options - {self.SYSTEMS}."
            self.log.error(m)
            raise ValueError(m)
        if any(
            [
                working_dir.startswith("../"),
                working_dir.endswith("/.."),
                "/../" in working_dir,
            ]
        ):
            msg = f"Inavlid working dir {working_dir} - Contains '..'."
            self.log.error(msg)
            raise ValueError(msg)

        # Connect to system
        self.log.info(f"Connecting {user} to system {system}...")
        self.user = user
        self.system = f"{system}.tacc.utexas.edu"
        self.connect(self.system, uid=user, pswd=psw, mfa_pswd=mfa)
        self.log.info(f"Succesfuly connected to {system}")

        # Initialize list of commands run to be empty
        self.commands = []

        # Get taccjm working directory, relative to users scratch directory
        self.scratch_dir = self.execute_command(
                f"echo {self.SCRATCH_DIR}")['stdout'].strip()
        self.log.info(
            "Resolved scratch path  to ${self.scratch_dir} for user {self.user}"
        )
        taccjm_dir = posixpath.join(self.scratch_dir, working_dir)
        self.trash_dir = posixpath.join(taccjm_dir, "trash")
        self.execute_command(f"mkdir -p {self.trash_dir}", wait=True)

    def execute_command(self, cmnd, wait=True, error=True) -> None:
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
        SSHCommandError 
            If command executed on TACC resource returns non-zero return code.
        """
        # Gets underlying transport object for main ssh connection.
        # Won't fail since not requesting a new conection yet.
        transport = self.get_transport()
        try:
            # This part will fail with SSH exception if request is refused
            channel = transport.open_session()
        except SSHException as ssh_error:
            # Will only occur if ssh connection is broken
            msg = "TACCJM ssh connection error: {ssh_error.__str__()}"
            self.log.error(msg)
            raise ssh_error

        channel.exec_command(cmnd)

        command_id = len(self.commands) + 1
        command_config = {
            "id": command_id,
            "cmd": cmnd,
            "ts": datetime.now(),
            "status": "STARTED",
            "stdout": "",
            "stderr": "",
            "history": [],
            "channel": channel,
        }
        self.commands.append(command_config)

        if wait:
            return self.process_command(command_id, wait=True, error=error)
        else:
            return command_config

    def process_command(
        self,
        command_id,
        nbytes: int = None,
        wait=False,
        error=True,
        max_nbytes: int = 1000000,
    ):
        """
        Process command

        Wait until a command finishes and read stdout and stderr. Raise error if
        anything in stderr, else return stdout.
        """
        ts = datetime.now()
        if command_id <= len(self.commands):
            command_config = self.commands[command_id - 1]
            if command_config["status"] not in ["COMPLETE", "FAILED"]:
                command_config["history"].append(
                    {"ts": command_config["ts"], "status": command_config["status"]}
                )
                if command_config["channel"].exit_status_ready() or wait:

                    command_config["rc"] = command_config["channel"].recv_exit_status()
                    command_config["stdout"] += (
                        command_config["channel"].recv(max_nbytes).decode("utf-8")
                    )
                    command_config["stderr"] += (
                        command_config["channel"]
                        .recv_stderr(max_nbytes)
                        .decode("utf-8")
                    )
                    command_config["channel"].close()
                    _ = command_config.pop("channel")
                    command_config["ts"] = ts
                    command_config["rt"] = (
                        to_datetime(ts)
                        - to_datetime(command_config["history"][0]["ts"])
                    ).seconds
                    if command_config["rc"] != 0:
                        command_config["status"] = "FAILED"
                        if error:
                            # Build SSH Command Error, only place this should be done
                            t = SSHCommandError(self.system, self.user, command_config)

                            # Only log the actual SSHCommandError object once, here
                            self.log.error(t.__str__())

                            # Update command list before erroring out
                            self.commands[command_id - 1] = command_config
                            raise t
                    else:
                        command_config["status"] = "COMPLETE"
                else:
                    if nbytes is not None:
                        command_config["stdout"] += (
                            command_config["channel"].recv(nbytes).decode("utf-8")
                        )
                    command_config["ts"] = ts
                    command_config["status"] = "RUNNING"
                self.commands[command_id - 1] = command_config
        else:
            raise ValueError(f"Invalid command_id {command_id}.")

        return command_config


    def _establish_sftp_connection(_self):
        """
        Wrapper for establishing sftp connetions and trapping errors to log
        an then exit gracefully
        """
        try:
            self.log.info(f'Opening sftp connection')
            sftp = self.open_sftp()
        except paramiko.ssh_exception.SSHException as e:
            msg = f"Error while opening SFTP connection: {e}"
            self.log.error(msg)
            raise e
        self.log.info(f'SFTP connection open')

        try:
            yield sftp
        finally:
            self.log.info(f'Closing SFTP connection')
            sftp.close()

    def _stat(self, sftp, path, follow_symbolic_links:bool = True):
       """
       Stat a file on remote system
       """
       stat = None
       try:
           # Query path to see if its directory or file
           if follow_symbolic_links:
               stat = sftp.stat(path)
           else:
               stat = sftp.lstat(path)
       except FileNotFoundError as f:
            msg = f"_stat - No such file or folder {f.filename}"
            self.log.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, path)
       except PermissionError as p:
            msg = f"_stat - Permission denied on {p.filename}"
            self.log.error(msg)
            raise PermissionError(errno.EACCES, msg, path)
       except SSHException as s:
            msg = "_state - Session is stale. Restart job manager."
            self.log.error(msg)
            raise s

       # Return list of dictionaries with file info
       return stat

    def list_files(self, path: str = ".", follow_symbolic_links:bool = True) -> List[dict]:
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
        SSHCommandError
            If can't access path for any reason on TACC system. This may be
            because the TACC user doesn't have permissions to view the given
            directory or that the path does not exist, for exmaple.

        """
        f_info = []
        f_attrs = ["st_atime", "st_gid", "st_mode", "st_mtime", "st_size", "st_uid"]

        # Open sftp connection
        self.log.info(f'list_files operation started on {path}')
        with self._estabish_sftp_connection() as sftp:
            self.log.info(f'Getting file info {path}')
            stat = self._stat(sftp, path,
                               follow_symbolic_links=follow_symbolic_links)

            if stat.S_ISDIR(stat.st_mode):
                # If directory get info on all files in directory
                f_attrs.insert(0, "filename")
                files = sftp.listdir_attr(path)
                self.log.info(f'Directory found, getting {len(flies)} files')
                for f in files:
                    # Extract fields from SFTPAttributes object for files
                    d = dict([(x, f.__getattribute__(x)) for x in f_attrs])
                    d["ls_str"] = f.asbytes()
                    f_info.append(d)
            else:
                # If file, just get file info
                self.log.info(f'File Found, returing file info')
                d = [(x, lstat.__getattribute__(x)) for x in f_attrs]
                d.insert(0, ("filename", path))
                d.append(("ls_str", lstat.asbytes()))
                f_info.append(dict(d))

        # Return list of dictionaries with file info
        return f_info

    def upload_file(self, local_file: str, remote_file: str) -> None:
        """
        Upload a file
        """
        # Sending directory -> Package into tar file
        if os.path.isfile(local_file):
            try:
                with self.open_sftp() as sftp:
                    sftp.put(local_file, remote_file)
            except FileNotFoundError as f:
                msg = f"upload - No such file or folder {f.filename}."
                self.log.error(msg)
                raise FileNotFoundError(errno.ENOENT, msg, f.filename)
            except PermissionError as p:
                msg = f"upload - Permission denied on {p.filename}"
                self.log.error(msg)
                raise PermissionError(errno.EACCES, msg, p.filename)
            except Exception as e:
                msg = f"upload - Unexpected error {e.__str__()}"
                self.log.error(msg)
                raise e
        elif os.path.isdir(local):
            raise ValueError(f"File {local_file} is not a file. Use upload or upload_dir")
        else:
              raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), local)


    def upload_dir(self, local_dir: str, remote: str, file_filter: str = "*") -> None:
        """
        Sends file or folder from local_dir path to remote path. If a file is
        specified, the remote path is the destination path of the file to be
        sent. If a folder is specified, all folder contents (recursive) are
        compressed into a .tar.gz file before being sent and then the contents
        are unpacked in the specified remote path.

        Parameters
        ----------
        local_dir : str
            Path to local_dir file or folder to send to TACC system.
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
            If local_dir file/folder does not exist, or remote destination path
            is invalid does not exist.
        PermissionError
            If user does not have permission to write to specified remote path
            on TACC system or access to local_dir file/folder and contents.
        SSHCommandError
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

        # Sending directory -> Package into tar file
        if os.path.isdir(local_dir):
            fname = os.path.basename(local_dir)
            local_tar_file = f".{fname}.taccjm.tar"
            remote_tar_file = f"{remote_dir}/.taccjm_temp_{fname}.tar"

            # TODO: Verify we have write permissions to remote_dir before we
            # do the work of compressing

            # Package tar file -> Recursive call
            tar_file(local_dir, local_tar_file,
                     arc_name=remote_tar_file, file_filter='*')

            # Send tar file
            try:
                self.upload_file(local_tar_file, remote_tar_file)
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
            try:
                self.execute_command(untar_cmd)
            except SSHCommandError as s:
                t.message = f"upload - Error unpacking tar file"
                self.log.error(t.message)
                raise t
        # Sending file
        elif os.path.isfile(local_dir):
            raise ValueError(f"File {local_dir} is not a directory. Use upload or upload_file")
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), local_dir)


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
        if os.path.isdir(local):
            self.upload_dir(local, remote)
        elif os.path.isfile(local):
            self.upload_file(local, remote)
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), local)


    def download_file(self, remote_file: str, local_file: str) -> None:
        """
        Downloads file from remote path on TACC resource to local
        path.

        Parameters
        ----------
        remote_file : str
            Unix-style path to file on TACC system to download.
            Note that path is relative to user's home directory.
        local_file : str
            Destination path of file

        Returns
        -------
        local_file : str
            Path of file downloaded

        Raises
        ------
        FileNotFoundError
            If local or remote file do not exist
        PermissionError
            If user does not have permission to write to specified remote path
            on TACC system or access to local file and contents.

        Warnings
        --------
        Will overwrite existing files Remote paths must use unix path seperator
        '/' since all TACC systems are unix.
        """

        local = os.path.abspath(local.rstrip("/"))
        remote = remote.rstrip("/")
        try:
            with self.open_sftp() as sftp:
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
                        self.execute_command(cmd)
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
                    self.execute_command(f"rm -rf {remote_tar}")
                else:
                    # Get remote file
                    sftp.get(remote, local)
        except FileNotFoundError as f:
            msg = f"download - No such file or folder {f.filename}."
            self.log.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"download - Permission denied on {p.filename}"
            self.log.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except TJMCommandError as t:
            msg = f"download - Error tar-ing remote file."
            t.message = msg
            self.log.error(msg)
            raise t


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
            with self.open_sftp() as sftp:
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
                        self.execute_command(cmd)
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
                    self.execute_command(f"rm -rf {remote_tar}")
                else:
                    # Get remote file
                    sftp.get(remote, local)
        except FileNotFoundError as f:
            msg = f"download - No such file or folder {f.filename}."
            self.log.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"download - Permission denied on {p.filename}"
            self.log.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except TJMCommandError as t:
            msg = f"download - Error tar-ing remote file."
            t.message = msg
            self.log.error(msg)
            raise t

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
            with self.open_sftp() as sftp:
                with sftp.open(path, "w") as jc:
                    if d_type == dict:
                        json.dump(data, jc)
                    else:
                        jc.write(data)
        except FileNotFoundError as f:
            msg = f"write - No such file or folder {f.filename}."
            self.log.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"write - Permission denied on {p.filename}"
            self.log.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except Exception as e:
            msg = f"write - Unknown error writing data to {path}: {e}"
            self.log.error(msg)
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
            with self.open_sftp() as sftp:
                with sftp.open(path, "r") as fp:
                    if data_type == "json":
                        data = json.load(fp)
                    else:
                        data = fp.read().decode("UTF-8")
            return data
        except FileNotFoundError as f:
            msg = f"read - No such file or folder {f.filename}."
            self.log.error(msg)
            raise FileNotFoundError(errno.ENOENT, msg, f.filename)
        except PermissionError as p:
            msg = f"read - Permission denied on {p.filename}"
            self.log.error(msg)
            raise PermissionError(errno.EACCES, msg, p.filename)
        except Exception as e:
            msg = f"read - Unknown reading data from {path}: {e}"
            self.log.error(msg)
            raise e

# Move this ssh client side
#     def remove(self, remote_path: str) -> None:
#         """
#         'Removes' a file/folder by moving it to the trash directory. Trash
#         should be emptied out preiodically with `empty_trash()` method.
#         Can also restore file `restore(path)` method.
# 
#         Parameters
#         ----------
#         remote_path : str
#             Unix-style path for the file/folder to send to trash. Relative to
#             home directory for user on TACC system if not an absolute path.
# 
#         Returns
#         -------
#         None
# 
#         Raises
#         ------
#         FileNotFoundError
#             If local file/folder does not exist, or remote destination path
#             is invalid does not exist.
#         PermissionError
#             If user does not have permission to modify specified remote path
#         TJMCommandError
#             If a directory is being sent, this error is thrown if there are any
#             issues unpacking the sent .tar.gz file in the destination directory.
# 
#         """
#         # Unix paths -> Get file remote file name and directory
#         file_name = remote_path.replace("/", "___")
#         trash_path = f"{self.trash_dir}/{file_name}"
#         abs_path = "./" + remote_path if remote_path[0] != "/" else remote_path
#         abs_path = posixpath.normpath(abs_path)
# 
#         # Check if path is a file or directory
#         is_dir = False
#         try:
#             with self.open_sftp() as sftp:
#                 fileattr = sftp.stat(abs_path)
#                 is_dir = stat.S_ISDIR(fileattr.st_mode)
#         except FileNotFoundError as f:
#             msg = f"remove - No such file/folder {abs_path}"
#             self.log.error(msg)
#             raise FileNotFoundError(errno.ENOENT, msg, abs_path)
# 
#         src_path = abs_path if not is_dir else abs_path + "/"
#         cmnd = f"rsync -a {src_path} {trash_path} && rm -rf {abs_path}"
#         try:
#             self.execute_command(cmnd)
#         except TJMCommandError as tjm_error:
#             tjm_error.message = f"remove - Unable to remove {remote_path}"
#             self.log.error(tjm_error.message)
#             raise tjm_error
# 
#     def restore(self, remote_path: str) -> None:
#         """
#         Restores a file/folder from the trash directory by moving it back to
#         its original path.
# 
#         Parameters
#         ----------
#         remote_path : str
#             Unix-style path of the file/folder that should not exist anymore to
#             restore.
# 
#         Returns
#         -------
#         None
# 
#         Raises
#         ------
#         FileNotFoundError
#             If the file to be restored does not exist in trash directory.
#         TJMCommandError
#             If error moving data from trash to its original path. This could
#             be becasue the original path still has data in it/exists.
#         """
#         # Unix paths -> Get file remote file name and directory
#         file_name = remote_path.replace("/", "___")
#         trash_path = posixpath.join(self.trash_dir, file_name)
#         abs_path = "./" + remote_path if remote_path[0] != "/" else remote_path
# 
#         # Check if trash path is a file or directory
#         is_dir = False
#         try:
#             with self.open_sftp() as sftp:
#                 fileattr = sftp.stat(trash_path)
#                 is_dir = stat.S_ISDIR(fileattr.st_mode)
#         except FileNotFoundError as f:
#             msg = f"restore - file/folder {file_name} not in trash."
#             self.log.error(msg)
#             raise FileNotFoundError(errno.ENOENT, msg, f.filename)
# 
#         src_path = f"{trash_path}/" if is_dir else trash_path
#         cmnd = f"rsync -a {src_path} {abs_path} && rm -rf {trash_path}"
#         try:
#             self.execute_command(cmnd)
#         except TJMCommandError as tjm_error:
#             tjm_error.message = f"restore - Unable to restore {remote_path}"
#             self.log.error(tjm_error.message)
#             raise tjm_error
# 
#     def empty_trash(self, filter_str: str = "*") -> None:
#         """
#         Cleans out trahs directly by permently removing contents with rm -rf
#         command.
# 
#         Parameters
#         ----------
#         filter : str, default='*'
#             Filter files in trash directory to remove
# 
#         Returns
#         -------
# 
#         """
#         self.execute_command(f"rm -rf {self.trash_dir}/{filter_str}")
# 
