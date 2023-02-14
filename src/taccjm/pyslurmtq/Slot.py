"""
Slot

Class defining abstract unit for job execution in a SLURM job.

"""
import time

class Slot:
    """
    Combination of (host, idx) that can run a SLURM task. A slot can have
    a task associated with it or be free. `host` corresponds to the name of the
    host that can execute the task, as accessible in the SLURM environment
    variable SLURM_JOB_NODELIST or, locally from the host, in SLURM_NODENAME.
    `idx` corresponds to the index in the total task list available to the
    SLURM job that the slot corresponds to. For example if a job is being run
    with 3 nodes and 5 total tasks (`-N 3 -n 5`), then the SLURM execution
    environment will look something like:

    .. code-block:: bash

        SLURM_JOB_NODELIST=c303-[005-006],c304-005
        SLURM_TASKS_PER_NODE=2(x2),1

    In this scenario, host `cs303-005` would have slot idxs 1 and
    2, `cs303-006` would have slots 3 and 4 associated with it, and host
    `cs304-005` would have only slot 5 associated with it. Note that these task
    slots do not corespond to the available CPUs per host available, which can
    vary depending on the cluster being used.

    Attributes
    ----------
    host : str
        Name of compute node on a SLURM execution system. Corresponds to a host 
        listed in the environment variable SLURM_JOB_NODELIST.
    idx : int
        Index of task in total available SLURM task slots. See above for more
        details.
    free : bool
        False if slot is being occupied currently by a task, True otherwise.
    tasks : List[:class:`Task`]
        List of Task objects that have been executed on this slot. If the
        slot is currently occupied, the last element in the list corresponds to
        the currently running task.
    """

    def __init__(self, host, idx):
        self.host = host
        self.idx = idx
        self.free = True
        self.tasks = []
        self.free_time = 0.0
        self.busy_time = 0.0
        self._last_ts = time.time()

    def occupy(self, task):
        """
        Occupy a slot with a task.

        Parameters
        ----------
        task: :class:Task
            Task object that will occupy the slot.

        Raises
        -------
        ValueError
            If trying to occupy a slot that is not currently free.

        """
        if not self.free:
            raise AttributeError(f"Trying to occupy a busy node {self}")
        now_time = time.time()
        self.free_time += now_time - self._last_ts
        self._last_ts = now_time
        self.tasks.append(task)
        self.free = False

    def release(self):
        """Make slot unoccupied."""
        now_time = time.time()
        self.busy_time += now_time - self._last_ts
        self._last_ts = now_time
        self.free = True

    def is_free(self):
        """Test whether slot is occupied"""
        return self.free

    def __str__(self):
        s = "FREE - " if self.free else "BUSY - "
        s += f"({self.host}, {self.idx}), tasks:{self.tasks}"
        return s

    def __repr__(self):
        s = f"Slot({self.host}, {self.idx})"
        return s

