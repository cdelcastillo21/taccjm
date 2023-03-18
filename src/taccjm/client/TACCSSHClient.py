"""
TACCSSHClient Class

A class that defines an ssh connection to a TACC system.


"""
import errno  # For error messages
import json  # For saving and loading job configs to disk
import os  # OS system utility functions for local system
import posixpath  # Paths on remote system (assumed UNIX)
import stat  # For reading file stat codes
import tarfile  # For sending compressed directories
import tempfile
from datetime import datetime  # Date time functionality
from pathlib import Path
from typing import List, Tuple, Union  # Type hints

from paramiko import AuthenticationException, BadHostKeyException, SSHException

from taccjm.constants import *  # For application configs
from taccjm.exceptions import SSHCommandError
from taccjm.client.SSHClient2FA import SSHClient2FA
from taccjm.utils import tar_file
from taccjm.log import logger

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


class TACCSSHClient(SSHClient2FA):
    """
    Class defining an ssh connection to a TACC system. tacc_ssh_server.py
    manages sets of these classes.

    Attributes
    ----------
    system : str
        Name of remote system to connect to. Supported systems: stampede2,
        ls5, frontera, maverick2
    user : str
        Name of tacc user connecting via ssh to resource.
    scratch_dir : str
        Unix-stye path to directory where unwanted files/folders go.

    Methods
    -------
    execute_command
    process_command
    process_active
    list_files
    upload
    download
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
    HOME_DIR = "$HOME"
    WORK_DIR = "$WORK"
    SCRATCH_DIR = "$SCRATCH"

    def __init__(
        self,
        system,
        user=None,
        psw=None,
        mfa=None,
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
        """
        # Initialize parent class with appropriate prompts for login to TACC systems
        super().__init__(
            user_prompt=self.USER_PROMPT,
            psw_prompt=self.PSW_PROMPT,
            mfa_prompt=self.MFA_PROMPT,
        )

        self.log = logger.bind(system=system, user=user)

        if system not in self.SYSTEMS:
            m = f"Unrecognized system {system}. Options - {self.SYSTEMS}."
            logger.error(m)
            raise ValueError(m)

        # Connect to system
        self.log.info(f"Connecting {user} to system {system}...")
        self.user = user
        self.system = f"{system}.tacc.utexas.edu"
        self.connect(self.system, uid=user, pswd=psw, mfa_pswd=mfa)
        self.log.info(f"Succesfuly connected to {system}")

        # Initialize list of commands run to be empty
        self.commands = []

        # get SCRATCH, HOME, and WORK dirs
        self._resolve_tacc_dirs()

    def _resolve_tacc_dirs(self):
        """
        Resolve HOME, SCRATCH, and WORK dirs for later usage.
        """
        cmnd = f"echo {self.HOME_DIR} {self.WORK_DIR} {self.SCRATCH_DIR}"
        self.home_dir, self.work_dir, self.scratch_dir = self.execute_command(
                cmnd)["stdout"].strip().split(' ')
        self.log.info(f"TACC dirs resolved to:\nwork : {self.work_dir}\n" +
                      f"home : {self.home_dir}\nscracth : {self.scratch_dir}")

    def _get_sftp(self):
        """
        Wrapper for establishing sftp connetions and trapping errors to log
        an then exit gracefully
        """
        try:
            self.log.info("Opening sftp connection")
            sftp = self.open_sftp()
        except SSHException as s:
            self.log.error(f"Error while opening SFTP connection: {s}")
            raise s
        self.log.info("SFTP connection open")

        return sftp

    def _sftp_exit(self, sftp, err, exc):
        """
        Close sftp, log error, and raise exception
        """
        sftp.close()
        self.log.error(err)
        raise exc

    def _stat(self, path, sftp=None, follow_symbolic_links: bool = True):
        """
        Stat a file on remote system
        """
        close = False
        if sftp is None:
            sftp = self._get_sftp()
            close = True
        stat = None
        try:
            # Query path to see if its directory or file
            if follow_symbolic_links:
                stat = sftp.stat(path)
            else:
                stat = sftp.lstat(path)
        except FileNotFoundError:
            self._sftp_exit(sftp, f"No such file or folder {path}", FileNotFoundError)
        except PermissionError:
            self._sftp_exit(sftp, f"Permission denied on {path}", PermissionError)
        except SSHException as s:
            self._sftp_exit(sftp, "SSH Error establishing connection", s)
            raise s

        if close:
            sftp.close()

        # Return listDEBUG o dictionaries with file info
        return stat

    def _decompress_remote(self, remote_tar_file: str, cdir: str = None):
        """
        De-compress a file remotely
        """
        untar_cmd = f"tar -xzvf {remote_tar_file}"
        if cdir is not None:
            untar_cmd += f" -C {cdir}"
        untar_cmd += f"; rm -rf {remote_tar_file}"
        try:
            self.log.info(
                "un-tarring the file remotely", extra={"untar_cmd": untar_cmd}
            )
            self.execute_command(untar_cmd)
        except SSHCommandError as s:
            self.log.error("Error unpacking tar file remotely")
            raise s

    def _compress_remote(self, remote_dir: str, file_filter: str = "*"):
        """
        Compres a file remotely
        """
        dirname, fname = posixpath.split(remote_dir)
        remote_tar = f"{self.scratch_dir}/.{fname}.tar.gz"

        # Build command. Filter files according to file filter
        cmd = f"cd {dirname} && "
        cmd += f"find {fname} -name '{file_filter}' -print0 | "
        cmd += f"tar -czvf {remote_tar} --null --files-from -"

        # Try packing remote tar file
        try:
            self.execute_command(cmd)
        except SSHCommandError as s:
            if "padding with zeros" in s.stdout:
                # Warning message, not an error.
                pass
            else:
                s.message = f"Error tar-ing {remote_tar}"
                raise s

        return remote_tar

    def _make_remote_absolute(self, path):
        """
        Makes remote path absolute if it is not. Relative paths are assumed to
        be relative to a user's scratch_dir.
        """
        path = path if posixpath.isabs(path) else posixpath.join(self.scratch_dir, path)
        return path

    def execute_command(self, cmnd,
                        wait=True,
                        fail=True,
                        key='SYSTEM'
                        ) -> None:
        """
        Executes a shell command through ssh on a TACC resource.

        Parameters
        ----------
        cmnd : str
            Command to execute. Be careful! 
        wait : bool, optional
            If set to true, wait for the command to finish. Default = True
        fail : bool, optional
            If set to true, exception will be raised upon failure of command.
            Default = True
        key : str, optiona;
            Key to tag the command with. Defaults to 'SYSTEM' for commands run
            internallby the class, but a user can tag commands with certain
            values to search/filter the command table based off the key.
            

        Returns
        -------
        cmnd_config : dict
            Dictionary containing command info.

        Raises
        ------
        SSHCommandError
            If command executed on TACC resource returns non-zero return code.
        """
        # Gets underlying transport object for main ssh connection.
        # Won't fail since not requesting a new conection yet.
        self.log.info("Opening channel to execute command")
        transport = self.get_transport()
        try:
            # This part will fail with SSH exception if request is refused
            channel = transport.open_session()
        except SSHException as ssh_error:
            # Will only occur if ssh connection is broken
            self.log.exception(
                    "TACCJM ssh connection error: {ssh_error.__str__()}")
            raise ssh_error

        self.log.info("Submitting command")
        channel.exec_command(cmnd)

        command_id = len(self.commands) + 1
        ts = datetime.now()
        command_config = {
            "id": command_id,
            "key": key,
            "cmd": cmnd,
            "ts": ts,
            "status": "STARTED",
            "stdout": "",
            "stderr": "",
            "history": [],
            "channel": channel,
            "rt": None,
            "fail": fail,
        }
        self.commands.append(command_config)

        self.log.info("Adding command to commands list",
                      extra={'config': command_config})

        if wait:
            self.log.info("Waiting for command to finish")
            return self.process_command(command_id, wait=True, error=fail)
        else:
            return command_config

    def process_command(
        self,
        command_id: int,
        nbytes: int = None,
        wait=False,
        error=False,
        max_nbytes: int = 1000000,
    ):
        """
        Process command

        Poll a command and if set, wait for it to finishes and read stdout and
        stderr. Raise error if rc != 0, the command that failed has fail=True,
        and error is set, else return completed cmnd config.

        Parameters
        ----------
        command_id : int
            ID of command to poll.
        nbytes : int, optional
            Read nbytes from the process. Note that if no output available, 
            will hang until output is available, and read up to nbytes.
        wait : bool, optional
            If set to true, wait for the command to finish. Default = True
        error : bool, optional
            If set to true, exception will be raised upon failure of command,
            that also has it fail parameter set to True. Default = False.
        """
        if command_id > len(self.commands) or command_id < 1:
            raise ValueError(f"Invalid id {command_id} (1<={len(self.commands)}).")
        command_config = self.commands[command_id - 1]

        if command_config["status"] in ["COMPLETE", "FAILED"]:
            return command_config

        prev_status = {"ts": command_config["ts"], "status": command_config["status"]}
        if command_config["channel"].exit_status_ready() or wait:
            command_config["history"].append(prev_status)
            command_config["rc"] = command_config["channel"].recv_exit_status()
            ts = datetime.now()
            command_config["stdout"] += (
                command_config["channel"].recv(max_nbytes).decode("utf-8")
            )
            command_config["stderr"] += (
                command_config["channel"].recv_stderr(max_nbytes).decode("utf-8")
            )
            command_config["channel"].close()
            _ = command_config.pop("channel")
            command_config["ts"] = ts
            command_config["rt"] = (ts - command_config["history"][0]["ts"]).seconds
            if command_config["rc"] != 0:
                command_config["status"] = "FAILED"
                self.commands[command_id - 1] = command_config
                if error and command_config["fail"]:
                    raise SSHCommandError(self.system,
                                          self.user, command_config)
            else:
                command_config["status"] = "COMPLETE"
        else:
            if nbytes is not None:
                command_config["stdout"] += (
                    command_config["channel"].recv(nbytes).decode("utf-8")
                )
            command_config["ts"] = datetime.now()
            command_config["status"] = "RUNNING"

            if prev_status["status"] != "RUNNING":
                command_config["history"].append(prev_status)

        self.commands[command_id - 1] = command_config

        return command_config

    def process_active(self, poll: bool = True, nbytes: int = None):
        """
        Process Active

        Get all active commands, and if poll is set, poll them to see if
        they've completed.

        Parameters
        ----------
        poll : bool, optional
            If set to True, will poll the commands to update their statuses.
            Note polling is done with no wait, unless nbytes is set.
        nbytes : int, optional
            Read nbytes from each active process. Note that if no output
            available, will hang until output is available, and read up to
            nbytes.
        """
        ds = ["COMPLETE", "FAILED"]
        active_commands = [c for c in self.commands if c["status"] not in ds]
        if not poll:
            return active_commands
        self.log.info(f"Polling {len(active_commands)} active commands.")
        still_active = []
        for cmnd in active_commands:
            res = self.process_command(
                cmnd["id"], wait=False, error=False, nbytes=nbytes
            )
            if res["status"] not in ds:
                still_active.append(res)

        self.log.info(f"Done polling. {len(still_active)} still active.")

        return still_active

    def list_files(
        self,
        path: str = None,
        follow_symbolic_links: bool = True,
        recurse: bool = False,
    ) -> List[dict]:
        """
        Returns the info on all files/folderes at a given path. If path is a
        file, then returns file info. If path is directory, then returns file
        info on all contents in directory.

        Parameters
        ----------
        path : str, default='$SCRATCH'
            Path to get file(s) info. If path isn't absolute, assumed to be
            relative to user's scratch directory,

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
        path = self._make_remote_absolute(path)

        # Open sftp connection
        self.log.info(f"list_files operation started on {path}")
        sftp = self._get_sftp()
        self.log.info(f"Getting file info {path}")
        fstat = self._stat(path, sftp=sftp, follow_symbolic_links=follow_symbolic_links)
        if stat.S_ISDIR(fstat.st_mode) and recurse:
            # If directory get info on all files in directory
            f_attrs.insert(0, "filename")
            files = sftp.listdir_attr(path)
            self.log.info(f"Directory found, getting {len(files)} files")
            for f in files:
                # Extract fields from SFTPAttributes object for files
                d = dict([(x, f.__getattribute__(x)) for x in f_attrs])
                d["ls_str"] = f.asbytes()
                f_info.append(d)
        else:
            # If file, just get file info
            self.log.info("File Found, returing file info")
            d = [(x, fstat.__getattribute__(x)) for x in f_attrs]
            d.insert(0, ("filename", path))
            d.append(("ls_str", fstat.asbytes()))
            f_info.append(dict(d))

        # Return list of dictionaries with file info
        sftp.close()
        return f_info

    def upload(self, local: str, remote: str, file_filter: str = "*") -> None:
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
        remote = self._make_remote_absolute(remote)

        # Make sure we are working with a directory
        if not os.path.exists(local):
            self.log.error(f"{local} does not exist.")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), local)

        self.log.info(f"Starting upload of {local} to {remote}")

        # TODO: Verify permissions to remote_dir? Check for overwrite scenario?

        # If directory
        isdir = os.path.isdir(local)
        if isdir:
            fname = os.path.basename(local)
            remote_parent, remote_fname = posixpath.split(remote)
            tempname = next(tempfile._get_candidate_names())
            local_tar_file = f".{fname}-{tempname}.taccjm.tar"
            self.log.info(f"Packing local tar file {local_tar_file}")
            tar_file(
                str(Path(local).resolve()),
                local_tar_file,
                arc_name=remote_fname,
                file_filter="*",
            )
            self.log.info("Succesfully compressed file")

            # Send tar file
            if remote_parent != "":
                destination = f"{remote_parent}/{local_tar_file}"
            else:
                destination = local_tar_file
            source = local_tar_file
        else:
            source = local
            destination = remote

        sftp = self._get_sftp()
        try:
            sftp.put(source, destination)
        except FileNotFoundError as f:
            self._sftp_exit(sftp, f"No such file or folder {f.filename}.", f)
        except PermissionError as p:
            self._sftp_exit(sftp, f"Permission denied on {p.filename}.", p)
        sftp.close()

        if isdir:
            os.remove(local_tar_file)
            self._decompress_remote(destination, cdir=remote_parent)

        self.log.info(f"Done uploading {local} to {remote}")

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
        SSHCommandError
            If a directory is being downloaded, this error is thrown if there
            are any issues packing the .tar.gz file on the remote system before
            downloading.

        Warnings
        --------
        Will overwrite existing files and folders and is recursive for folders
        being downloaded. Remote paths must use unix path seperator '/' since
        all TACC systems are unix.
        """
        remote = self._make_remote_absolute(remote)

        local = os.path.abspath(local.rstrip(os.sep))
        remote = remote.rstrip("/")

        self.log.info(f"Starting download of folder {remote} to {local}")

        self.log.info("Getting info on path {remote}")
        sftp = self._get_sftp()
        fileattr = sftp.stat(remote)
        is_dir = stat.S_ISDIR(fileattr.st_mode)
        if is_dir:
            # Tar remote file
            remote_tar_file = self._compress_remote(remote, file_filter)
            to_download = remote_tar_file
            download_path = f"{local}.tar.gz"
        else:
            to_download = remote
            download_path = local

        self.log.info(f"Download of {to_download} to {download_path}")
        try:
            sftp.get(to_download, download_path)
        except FileNotFoundError as f:
            self._sftp_exit(sftp, "No such file or folder.", f)
        except PermissionError as p:
            self._sftp_exit(sftp, "Permission denied.", p)

        # Done with sftp connection
        sftp.close()

        if is_dir:
            # unpack tar file locally
            with tarfile.open(download_path) as tar:
                tar.extractall(path=local)

            # Remove local and remote tar files
            os.remove(download_path)
            self.execute_command(f"rm -rf {remote_tar_file}")

        self.log.info(f"Download complete of {remote} to {local}")

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
        path = self._make_remote_absolute(path)

        d_type = type(data)
        if d_type not in [dict, str]:
            raise ValueError(f"Data type {d_type} is not supported")
        try:
            self.log.info(f"Writing to {path}")
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
        path = self._make_remote_absolute(path)

        if data_type not in ["json", "text"]:
            raise ValueError(f"read - data type {data_type} is not supported")
        try:
            self.log.info(f"Reading {data_type} from {path}")
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
