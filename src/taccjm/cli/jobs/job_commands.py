"""
TACCJM Jobs CLI
"""
import pdb
import re
from pathlib import Path

import click
from prettytable import PrettyTable

import taccjm.taccjm_client as tjm
from taccjm.exceptions import TACCJMError
from taccjm.utils import filter_res, format_job_dict

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

def _get_default():
    jms = tjm.list_jms()
    if len(jms) != 1:
        raise TACCJMError('More than one or no job managers intialized.')
    return jms[0]['jm_id']

@click.group(short_help="list/deploy/submit/cancel/remove/restore")
@click.option("--jm_id", default=None,
              help="Job Manager to execute operation on. Defaults to first available.")
@click.pass_context
def jobs(ctx, jm_id):
    """
    TACC Job Manager Jobs

    TACCJM Job operations. If no jm_id is specified, then jobs are on system
    connected to by first job manager in a `taccjm list` operation.
    """
    ctx.ensure_object(dict)
    ctx.obj['jm_id'] = jm_id

@jobs.command(short_help="List jobs deployed.")
@click.option("--match", default=r".", show_default=True,
              help="Regular expression to search job ids on.")
@click.pass_context
def list(ctx, match):
    """
    List jobs

    List jobs deployed on job manager. Can filter results using --match option.
    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    res = [{"job_id": j} for j in tjm.list_jobs(jm_id)]
    str_res = filter_res(res, ["job_id"], search="job_id", match=match)
    click.echo(str_res)


@jobs.command(short_help="Deploy a job",
              context_settings=dict(ignore_unknown_options=True,
                                    allow_extra_args=True))
@click.option("--config_file", type=str, default="job.json", show_default=True,
              help="Path to job config json file.")
@click.option("-s/-ns", "--stage/--no-stage", default=False, show_default=True,
              help="Whether to actually stage the job on the remote system.")
@click.pass_context
def deploy(ctx, config_file, stage):
    """
    Deploy Job

    Deploy a job to a remote system. Job config is assume to be in a json file
    with the name `job.json`. Change path to json file with --config_file.
    Prints job config.
    """
    kwargs = dict([(ctx.args[i][2:], ctx.args[i+1]) for i in range(0, len(ctx.args), 2)])
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    job = tjm.deploy_job(
        jm_id,
        job_config=None,
        local_job_dir=str(Path(config_file).absolute().parent),
        job_config_file=str(Path(config_file).name),
        stage=stage,
        **kwargs
    )
    str_res = format_job_dict(job)
    click.echo(str_res)


@jobs.command(short_help="Submit job to SLURM job queue.")
@click.argument("job_id", type=str)
@click.pass_context
def submit(ctx, job_id):
    """
    Submit Job

    Submits a job JOB_ID to SLURM queue. Note job must be deployed first to be
    submitted.
    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    job = tjm.submit_job(jm_id, job_id)
    str_res = format_job_dict(job)
    click.echo(str_res)


@jobs.command(short_help="Cancel job in SLURM job queue.")
@click.argument("job_id", type=str)
@click.pass_context
def cancel(ctx, job_id):
    """
    Cancel Job

    Cancel a job JOB_ID that has been submitted to the SLURM task queue.
    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    job = tjm.cancel_job(jm_id, job_id)
    str_res = format_job_dict(job)
    click.echo(str_res)


@jobs.command(short_help="Send a job directory to trash.")
@click.argument("job_id", type=str)
@click.pass_context
def remove(ctx, job_id):
    """
    Remove Job

    Delete job JOB_ID from job directory by moving it to the trash directory.
    Note the job can be restored still with the restore command.
    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    _ = tjm.remove_job(jm_id, job_id)
    click.echo(f'Job {job_id} succsefully moved to trash')

@jobs.command(short_help="Restore job directory from trash.")
@click.argument("job_id", type=str)
@click.pass_context
def restore(ctx, job_id):
    """
    Restore Job

    Restore JOB_ID that has been moved to the trash directory previously via
    a remove command.
    """
    jm_id = ctx.obj['jm_id'] if ctx.obj['jm_id'] is not None else _get_default()
    job = tjm.restore_job(jm_id, job_id)
    str_res = format_job_dict(job)
    click.echo(str_res)

