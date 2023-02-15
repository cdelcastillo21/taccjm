"""
Ensemble TACC Simulation

"""
import logging

from taccjm.pyslurmtq.SLURMTaskQueue import SLURMTaskQueue
from taccjm.TACCSimulation import TACCSimulation


class EnsembleTACCSimulation(TACCSimulation):
    """A simulation class representing a set of ADCIRC runs.
    """

    JOB_DEFAULTS = {
        "allocation": None,
        "node_count": 1,
        "processors_per_node": 48,
        "max_run_time": 0.2,
        "queue": "development",
        "dependencies":  [],
    }

    # These are file/folder inputs needed to run the simulation
    ARGUMENTS = [
            {
                "name": "task_file",
                "type": "input",
                "desc": "Input file with runs for ensemble.",
                "required": False,
            },
            {
                "name": "tasks",
                "type": "apram",
                "desc": "List of tasks for ensemble",
                "default": [],
            },
            {
                "name": "task_max_runtime",
                "type": "param",
                "desc": "Max runtime (in hours) for tasks in ensemble.",
                "default": 0.1,
            },
            {
                "name": "max_runtime",
                "type": "param",
                "desc": "Max runtime (in hours) for the whole ensemble.",
                "default": 0.2,
            },
            {
                "name": "summary_interval",
                "type": "param",
                "desc": "Interval (in seconds) of output of task-queue summary",
                "default": 60,
            },
    ]

    def __init__(self,
                 name: str = None,
                 system: str = None,
                 log_config: dict = None,
                 ):
        super().__init__(name, system, log_config,
                         script_file=__file__,
                         class_name=__name__)

    def _validate_run_configs(self, runs):
        """
        TODO: Implement run config validation
        """
        pass

    def generate_runs(self):
        """
        Add runs using current job_config here. This will run from job
        execution time. For now we just return the task list from the config
        """
        return []

    def setup_job(self):
        """
        Command to set-up job directory.

        Create a directory for each job.
        """
        self.log.info("Starting job set-up")
        self.client.exec('mkdir -p runs && mkdir -p outputs')
        self.log.info("Job set-up done")

    def run_job(self):
        """
        Job run entrypoint
        """
        self.log.info("Starting Ensemble Simulation. Initializing task list")
        tasks = self.job_config['args']['tasks']
        if 'task_file' in self.job_config['args']:
            tasks.append(self.client.read(
                self.job_config['args']['task_file']))
        self._validate_run_configs(tasks)
        tasks = tasks + self.generate_runs()
        self.log.info(f"Found {len(tasks)} valid tasks", extra={'tasks': tasks})

        self.log.info("Initializing task queue")
        tq = SLURMTaskQueue(
            workdir=self.client.job_path(self.job_config['job_id'],
                                         'task-queue'),
            task_max_runtime=60*60*self.job_config['args']['task_max_runtime'],
            max_runtime=60*60*self.job_config['args']['max_runtime'],
            delay=1,
            loglevel=logging.DEBUG,
            summary_interval=self.job_config['args']['summary_interval'],
        )

        iteration = 0
        self.log.info("Starting execution loop")
        while len(tasks) > 0:
            self.log.info(f"Putting {len(tasks)} into queue",
                          extra={'iter': iteration, 'tasks': tasks})
            tq.enqueue(tasks)
            self.log.info("Running queue...")
            tq.run()
            self.log.info("Ensemble Simulation Done")
            self.log.info("Generating tasks for next iteration")
            tasks = self.generate_runs()
