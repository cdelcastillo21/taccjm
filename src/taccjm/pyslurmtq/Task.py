"""
Task Class

TODO: Description
"""
import os
import pdb
import stat
import subprocess
import linecache as lc
import time
from pathlib import Path

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

class Task:
    """
    Command to be executed in parallel using ibrun on a slot of SLURM tasks as
    designated by :class:SLURMTaskQueue. This class contains the particulars of
    a task to be executing, including the main parallel command to be executed
    in parallel using ibrun, optional pre/post process commands to be executed
    serially, and an optional directory to change to before executing the main
    parallel command. Once appropriate resources for the task have been found,
    and the execute() method is called, the class `slots` attribute will be
    filled with an `(offset, extent)` pair indicating what continuous region of
    the available task slots is being occupied by the currently running task.
    The command to be executed is then wrapped into a script file that is stored
    in `workdir` and a :class:subprocess.Popen object is opened to execute the
    script.

    Note that the :class:SLURMTaskQueue handles the initialization and
    management of task objects, and in general a user has no need to initialize
    task objects individually.

    Attributes
    ----------
    task_id : int
        Unique task ID to assign to this Task.
    cmnd : str
        Main command to be wrapped in `ibrun` with the appropriate offset/extent
        parameters for parallel execution.
    cores : int
        Number of cores, which correspond to SLURM job task numbers, to use for
        the job.
    pre : str
        Command to be executed in serial before the main parallel command.
    post : str
        Command to be executed in serial after the main parallel command.
    cdir: str
        directory to change to before executing the main parallel command.
    workdir : str
        directory to store execution script, along with output and error files.
    execfile : str
        Path to shell script containing wrapped command to be run by the
        subprocess that is spawned to executed the SLURM task. Note this file
        won't exist until the task is executed.
    logfile : str
        Path to file where stdout of the SLURM task will be redirected to. Note
        this file won't exist until the task is executed.
    errfile : str
        Path to file where stderr of the SLURM task will be redirected to. Note
        this file won't exist until the task is executed.
    slots : Tuple(int)
        `(offset, extent)` tuple in SLURM Task slots where task is currently
        being/was executed, or None if task has not been executed yet.
    start_ts : float
        Timestamp, in seconds since epoch, when task execution started, or None
        if task has not been executed  yet.
    end_ts : float
        Timestamp, in seconds since epoch, when task execution finished, as
        measured by first instance the process is polled using `get_rc()` with a
        non-negative response, or None if task has not finished yet.
    running_time : float
        Timestamp, in seconds since epoch, when task execution finished, as
        measured by first instance the process is polled using `get_rc()` with a
        non-negative response, or None if task has not finished yet.
    """
    def __init__(self,
            task_id: int,
            cmnd: str,
            workdir: str,
            cores: int=1,
            pre: str=None,
            post: str=None,
            cdir: str=None):

        self.task_id = task_id
        self.command = cmnd
        self.cores = int(cores)
        self.pre = pre
        self.post = post
        self.cdir = cdir

        if self.cores <= 0:
            raise ValueError(f'Cores for task must be >=0')

        self.workdir = Path(workdir)
        self.workdir.mkdir(exist_ok=True)
        self.logfile = self.workdir / f"{task_id}-log"
        self.errfile = self.workdir / f"{task_id}-err"
        self.execfile = self.workdir / f"{task_id}-exec"

        self.slots = None
        self.start_ts = None
        self.end_ts = None
        self.running_time = None
        self.rc = None
        self.err_msg = None
        self.pid = None
        self.sub_proc = None

    def __str__(self):
        s = f"Task(id:{self.task_id}, cmnd:<<{self.command}>>, cores:{self.cores}, "
        s += f"pre:<<{self.pre}>>, post:<<{self.post}>>, cdir:{self.cdir})"
        return s

    def __repr__(self):
        s = f"Task({self.task_id}, '{self.command}', {self.workdir}, "
        s += f"cores={self.cores}"
        if self.pre is not None:
            s += f", pre='{self.pre}'"
        if self.post is not None:
            s += f", post='{self.post}'"
        if self.cdir is not None:
            s += f", cdir='{self.cdir}'"
        s += ")"
        return s

    def _wrap(self, offset, extent):
        """Take a commandline, write it to a small file, and return the
        commandline that sources that file
        """
        f = open(self.execfile, "w")
        f.write("#!/bin/bash\n\n")
        f.write(f"cd {self.workdir}\n")
        if self.pre is not None:
            f.write(f"{self.pre}\n")
            f.write("if [ $? -ne 0 ]\n")
            f.write("then\n")
            f.write("  exit 1\n")
            f.write("fi\n")
        if self.cdir is not None:
            f.write(f"cd {self.cdir}\n")
            f.write(f"cwd=$(pwd)\n")
        f.write(f"ibrun -o {offset} -n {extent} {self.command}\n")
        if self.cdir is not None:
            f.write(f"cd $cwd\n")
        f.write("if [ $? -ne 0 ]\n")
        f.write("then\n")
        f.write("  exit 1\n")
        f.write("fi\n")
        if self.post is not None:
            f.write(f"{self.post}\n")
            f.write("if [ $? -ne 0 ]\n")
            f.write("then\n")
            f.write("  exit 1\n")
            f.write("fi\n")
        f.close()
        os.chmod(
            self.execfile,
            stat.S_IXUSR
            + +stat.S_IXGRP
            + stat.S_IXOTH
            + stat.S_IWUSR
            + +stat.S_IWGRP
            + stat.S_IWOTH
            + stat.S_IRUSR
            + +stat.S_IRGRP
            + stat.S_IROTH,
        )

        new_command = f"{self.execfile} > {self.logfile} 2> {self.errfile}"

        return new_command

    def execute(self, offset, extent):
        """
        Execute a wrapped command on subprocesses given a task slot range.

        Parameters
        ----------
        offset : int
            Offset in list of total available SLURM tasks available. This will
            determine the `-o` paraemter to run ibrun with.
        extent : int
            Extent, or number of slots, to occupy in list of total available
            SLURM tasks available. This will determine the `-n` parameter to
            run ibrun with.

        """
        self.start_ts = time.time()
        self.slots = (offset, extent)
        self.sub_proc = subprocess.Popen(
            self._wrap(offset, extent), shell=True, stdout=subprocess.PIPE
        )
        self.pid = self.sub_proc.pid

    def terminate(self):
        """Terminate subprocess executing task if it exists."""
        if self.sub_proc is not None:
            self.sub_proc.terminate()

    def get_rc(self):
        """Poll process to see if completed"""
        self.rc = self.sub_proc.poll()
        if self.rc is not None:
            self.end_ts = time.time()
            self.running_time = self.end_ts - self.start_ts
            if self.rc > 0:
                self.err_msg = self.read_err(lineno=1)[0]
            return self.rc
        else:
            self.running_time = time.time() - self.start_ts

        return self.rc

    def _read_file(self, fname, lineno=None):
        """Read Task output file"""
        output = None
        if self.start_ts is not None:
            if lineno is None:
                with open(fname) as f:
                    output = f.readlines()
            elif type(lineno) == list:
                output = []
                for ln in lineno:
                    output.append(lc.getline(fname, ln))
            else:
                output = [lc.getline(fname, lineno)]
        return output

    def read_log(self, lineno=None):
        """Read Task output file"""
        return self._read_file(str(self.logfile.resolve()), lineno=lineno)

    def read_err(self, lineno=None):
        """Read Task error file"""
        return self._read_file(str(self.errfile.resolve()), lineno=lineno)

    def read_task_file(self, fname, lineno=None):
        """Read Task error file"""
        fpath = Path(self.workdir / fname)
        fname = fpath.name
        if not fpath.exists():
            raise ValueError(f'Task file at {fname} at {fpath} does not exist')

        return self._read_file(str(fpath.resolve()), lineno=lineno)




