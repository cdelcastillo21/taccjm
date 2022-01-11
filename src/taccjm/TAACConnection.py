""" TACC Connection Class


"""

import os                       # OS system utility functions for local system
import posixpath                # Paths on remote system (assumed UNIX)
import errno                    # For error messages
import tarfile                  # For sending compressed directories
import re                       # Regular Expressions
import pdb                      # Debug
import json                     # For saving and loading job configs to disk
import time                     # Time functions
import logging                  # Used to setup the Paramiko log file
import stat                     # For reading file stat codes
from datetime import datetime   # Date time functionality
from fnmatch import fnmatch     # For unix-style filename pattern matching
from taccjm.utils import *      # TACCJM Util functions for config files/dicts
from taccjm.constants import *  # For application configs
from typing import Tuple, List  # Type hints
from typing import Union        # Type hints

# Modified paramiko ssh client and common paramiko exceptions
from taccjm.SSHClient2FA import SSHClient2FA
from paramiko import SSHException, AuthenticationException, BadHostKeyException

# Custom exception for handling remote command errors
from taccjm.exceptions import TJMCommandError

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


logger = logging.getLogger(__name__)


class TACCConnection():
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

    Notes
    -----
    All remote paths should use Unix path seperateor '/'.

    """

    ROOT = 'tacc.utexas.edu'
    SYSTEMS = ['stampede2', 'ls6', 'frontera', 'maverick2']
    USER_PROMPT = "Username:"
    PSW_PROMPT = "Password:"
    MFA_PROMPT ="TACC Token Code:"
    SCRATCH_DIR = "$SCRATCH"


    def __init__(self, system, user=None,
            psw=None, mfa=None, working_dir='taccjm'):
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
        if any([working_dir.startswith('../'),
                working_dir.endswith('/..'),
                '/../' in working_dir ]):
            msg = f"Inavlid working dir {working_dir} - Contains '..'."
            logger.error(msg)
            raise ValueError(msg)

        # Connect to system
        logger.info(f"Connecting {user} to system {system}...")
        self.system= f"{system}.tacc.utexas.edu"
        self._client = SSHClient2FA(user_prompt=self.USER_PROMPT,
                psw_prompt=self.PSW_PROMPT,
                mfa_prompt=self.MFA_PROMPT)
        self.user = self._client.connect(self.system,
                uid=user, pswd=psw, mfa_pswd=mfa)
        logger.info(f"Succesfuly connected to {system}")

        # Get taccjm working directory, relative to users scratch directory
        logger.info("Resolving path ${self.SCRATCH_DIR} for user {self.user}")
        self.SCRATCH_DIR = scratch_dir = self.execute_command(
                f"echo {self.SCRATCH_DIR}").strip()
        taccjm_dir = posixpath.join(self.SCRATCH_DIR, working_dir)

        # Initialze jobs, apps, scripts, and trash dirs
        logger.info("Creating if jobs, apps, scripts, and trash dirs")
        for d in zip(['jobs_dir', 'apps_dir', 'scripts_dir', 'trash_dir'],
                      ['jobs', 'apps', 'scripts', 'trash']):
            setattr(self, d[0], posixpath.join(taccjm_dir,d[1]))
            self._mkdir(getattr(self, d[0]), parents=True)

        # Get python path and home dir
        self.python_path = self.execute_command('which python')


    def execute_command(self, cmnd) -> None:
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
        try:
            stdin, stdout, stderr = self._client.exec_command(cmnd)
        except SSHException as ssh_error:
            # Will only occur if ssh connection is broken
            msg = "TACCJM ssh connection error: {ssh_error.__str__()}"
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
            ret = self.execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = "_mkdir - Could not create directory"
            logger.error(tjm_error.message)
            raise tjm_error


    def list_files(self, path:str='.') -> List[dict]:
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
            f_attrs = ['st_atime', 'st_gid', 'st_mode',
                    'st_mtime', 'st_size', 'st_uid']

            # Open sftp connection
            with self._client.open_sftp() as sftp:
                # Query path to see if its directory or file
                lstat = sftp.lstat(path)

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
        except Exception as e:
            msg = f"list_files - Unknown error trying to access {path}: {e}"
            logger.error(msg)
            raise e


    def peak_file(self, path:str, head:int=-1, tail:int=-1) -> str:
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
        if head>0:
            cmnd = f"head -{head} {path}"
        elif tail>0:
            cmnd = f"tail -{tail} {path}"
        else:
            cmnd = f"head {path}"
        try:
            ret = self.execute_command(cmnd)
        except TJMCommandError as t:
            f_not_found = ['No such file or directory', 'Not a directory']
            if 'Permission denied' in t.stderr:
                msg = f"peak_file - Dont have permission to access {path}"
                raise PermissionError(errno.EACCES, msg, path)
            elif any([x in t.stderr or x in t.stdout for x in f_not_found]):
                msg = f"peak_file - No such file or directory {path}"
                logger.error(msg)
                raise FileNotFoundError(errno.ENOENT,
                        os.strerror(errno.ENOENT), path)
            else:
                t.message = f"peak_file - Unexpected error."
                logger.error(t.message)
                raise t

        return ret


    def upload(self, local:str, remote:str, file_filter:str='*') -> None:
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
        remote_dir = '.' if remote_dir=='' else remote_dir

        try:
            # Sending directory -> Package into tar file
            if os.path.isdir(local):
                fname = os.path.basename(local)
                local_tar_file = f".{fname}.taccjm.tar"
                remote_tar_file = f"{remote_dir}/.taccjm_temp_{fname}.tar"

                # Package tar file -> Recursive call
                with tarfile.open(local_tar_file, "w:gz") as tar:
                    f = lambda x : x if fnmatch(x.name, file_filter) else None
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
                _ = self.execute_command(untar_cmd)
            # Sending file
            elif os.path.isfile(local):
                with self._client.open_sftp() as sftp:
                    res = sftp.put(local, remote)
            else:
                raise FileNotFoundError(errno.ENOENT,
                        os.strerror(errno.ENOENT), local)
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


    def download(self, remote:str, local:str, file_filter:str='*') -> None:
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

        local = os.path.abspath(local.rstrip('/'))
        remote = remote.rstrip('/')
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
                        self.execute_command(cmd)
                    except TJMCommandError as t:
                        if 'padding with zeros' in t.stdout:
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


    def remove(self, remote_path:str) -> None:
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
        file_name = remote_path.replace('/','___')
        trash_path = f"{self.trash_dir}/{file_name}"
        abs_path = './' + remote_path if remote_path[0]!='/' else remote_path
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

        src_path  = abs_path if not is_dir else abs_path + '/'
        cmnd = f"rsync -a {src_path} {trash_path} && rm -rf {abs_path}"
        try:
            ret = self.execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = f"remove - Unable to remove {remote_path}"
            logger.error(tjm_error.message)
            raise tjm_error


    def restore(self, remote_path:str) -> None:
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
        file_name = remote_path.replace('/','___')
        trash_path = posixpath.join(self.trash_dir, file_name)
        abs_path = './' + remote_path if remote_path[0]!='/' else remote_path

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
            ret = self.execute_command(cmnd)
        except TJMCommandError as tjm_error:
            tjm_error.message = f"restore - Unable to restore {remote_path}"
            logger.error(tjm_error.message)
            raise tjm_error


    def empty_trash(self, filter_str:str='*') -> None:
        """
        Cleans out trash directly by permently removing contents with rm -rf
        command.

        Parameters
        ----------
        filter : str, default='*'
            Filter files in trash directory to remove

        Returns
        -------

        """
        self.execute_command(f"rm -rf {self.trash_dir}/{filter_str}")


    def write(self, data:Union[str, dict], path:str) -> None:
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
                with sftp.open(path, 'w') as jc:
                    if d_type==dict:
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


    def read(self, path:str, data_type:str='text') -> Union[str, dict]:
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
        if data_type not in ['json', 'text']:
            raise ValueError(f"read - data type {data_type} is not supported")
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(path, 'r') as fp:
                    if data_type=='json':
                        data = json.load(fp)
                    else:
                        data = fp.read().decode('UTF-8')
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


