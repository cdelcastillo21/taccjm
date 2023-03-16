"""
TACCJM Jobs CLI
"""
from pathlib import Path

import rich_click as click

from taccjm.cli.utils import _get_client, _get_files_str
from taccjm.utils import filter_res, format_job_dict

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


@click.group(short_help="list/deploy/submit/cancel/remove/restore")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="SSH Connection to execute operation on. Defaults to first available.",
)
@click.pass_context
def jobs(ctx, conn_id):
    """
    TACC Job Manager Jobs

    TACCJM Job operations. If no conn_id is specified, then jobs are on system
    connected to by first connection in a `taccjm list` operation.
    """
    ctx.ensure_object(dict)
    ctx.obj["client"] = _get_client(conn_id)


@cli.command(short_help="Show SLURM job queue on JM_ID for USER.")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="TACC connection id to show queue for. If none specified, then "
    + "defaults to first connection from a `taccjm list` command.",
)
@click.option(
    "-u",
    "--user",
    default=None,
    help="User to show job queue for. Use `all` ti show queue for all users",
)
@click.option(
    "-s",
    "--search",
    type=click.Choice(_queue_fields, case_sensitive=False),
    help="Column to search.",
    show_default=True,
    default="username",
)
@click.option(
    "-m",
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to match.",
)
def showq(conn_id, user, search, match):
    """
    Show TACC SLURM Job Queue

    Returns the active SLURM Job queue on TACC system that JM_ID is connected
    to, with fields:\n
        - job_id : SLURM job id. NOT TACCJM Job ID.\n
        - job_name : SLURM job name.\n
        - username : User that launched job.\n
        - state : State of job.\n
        - nodes : Nodes request.\n
        - remaining : Time remaining in job if running.\n
        - start_time : Start time of job.

    Use `--search` and `--match` flags to filter results.
    """
    client = _get_client(conn_id)
    res = client.showq(user=user)
    str_res = build_table(res, _queue_fields)
    click.echo(f"SLURM Queue for {user} on {conn_id}:")
    click.echo(str_res)


@cli.command()
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="TACC connection id to get allocatiosn for. If none specified, then "
    + "defaults to first connection from a `taccjm list` command.",
)
@click.option(
    "--search",
    type=click.Choice(_allocation_fields, case_sensitive=False),
    help="Column to search.",
    show_default=True,
    default="name",
)
@click.option(
    "--match", default=r".", show_default=True, help="Regular expression to match."
)
def allocations(conn_id, search, match):
    """
    Get TACC Allocations

    Return TACC allocations on system connected to by conn_id. For each
    allocation, returns remaining SUs. if no conn_id specified, defaults to
    first SSH connection in `taccjm list`.
    """
    client = _get_client(conn_id)
    res = client.get_allocations()
    str_res = build_table(res, _allocation_fields, search, match)
    click.echo(f"Allocations for {conn_id}:")
    click.echo(str_res)


@jobs.command(short_help="List jobs deployed.")
@click.option(
    "-n",
    "--max_num_jobs",
    default=10,
    show_default=True,
    help="Max number of jobs to display",
)
@click.option(
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to search job ids on.",
)
@click.pass_context
def list(ctx, match, max_num_jobs):
    """
    List jobs

    List jobs deployed on job manager. Can filter results using --match option.
    """
    client = ctx.obj["client"]
    str_res = _get_files_str(
        client,
        client.jobs_dir,
        attrs=['name', 'modified_time'],
        recurse=True,
        hidden=False,
        search='name',
        match=match,
        fnames=True,
        max_n=max_num_jobs,
    )
    click.echo(f'Jobs on {client.id} at {client.scripts_dir}:')
    click.echo(str_res)


@jobs.command(
    short_help="Deploy a job",
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
)
@click.option(
    "--config_file",
    type=str,
    default="job.json",
    show_default=True,
    help="Path to job config json file.",
)
@click.option(
    "-s/-ns",
    "--stage/--no-stage",
    default=False,
    show_default=True,
    help="Whether to actually stage the job on the remote system.",
)
@click.pass_context
def deploy(ctx, config_file, stage):
    """
    Deploy Job

    Deploy a job to a remote system. Job config is assume to be in a json file
    with the name `job.json`. Change path to json file with --config_file.
    Prints job config.
    """
    kwargs = dict(
        [(ctx.args[i][2:], ctx.args[i + 1]) for i in range(0, len(ctx.args), 2)]
    )
    jm_id = ctx.obj["jm_id"] if ctx.obj["jm_id"] is not None else _get_default()
    job = tjm.deploy_job(
        jm_id,
        job_config=None,
        local_job_dir=str(Path(config_file).absolute().parent),
        job_config_file=str(Path(config_file).name),
        stage=stage,
        **kwargs,
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
    jm_id = ctx.obj["jm_id"] if ctx.obj["jm_id"] is not None else _get_default()
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
    jm_id = ctx.obj["jm_id"] if ctx.obj["jm_id"] is not None else _get_default()
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
    jm_id = ctx.obj["jm_id"] if ctx.obj["jm_id"] is not None else _get_default()
    _ = tjm.remove_job(jm_id, job_id)
    click.echo(f"Job {job_id} succsefully moved to trash")


@jobs.command(short_help="Restore job directory from trash.")
@click.argument("job_id", type=str)
@click.pass_context
def restore(ctx, job_id):
    """
    Restore Job

    Restore JOB_ID that has been moved to the trash directory previously via
    a remove command.
    """
    jm_id = ctx.obj["jm_id"] if ctx.obj["jm_id"] is not None else _get_default()
    job = tjm.restore_job(jm_id, job_id)
    str_res = format_job_dict(job)
    click.echo(str_res)


