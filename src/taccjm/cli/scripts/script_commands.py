"""
TACCJM Scripts CLI
"""
from pathlib import Path

import rich_click as click

from taccjm.cli.utils import _get_client, _get_files_str
from taccjm.utils import filter_res

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


@click.group(short_help="list/deploy/run")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="Job Manager to execute operation on. Defaults to first available.",
)
@click.pass_context
def scripts(ctx, conn_id):
    """
    TACC Job Manager Scripts

    CLI Entrypoint for commands related to scripts.
    """
    client = _get_client(conn_id)
    ctx.ensure_object(dict)
    ctx.obj["client"] = client


@scripts.command(short_help="List deployed scripts.")
@click.option(
    "-m",
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to search script names for.",
)
@click.pass_context
def list(ctx, match):
    """
    List scripts deployed on JM_ID
    """
    client = ctx.obj["client"]
    str_res = _get_files_str(
        client,
        client.scripts_dir,
        attrs=['name', 'modified_time'],
        recurse=True,
        hidden=False,
        search=['name'],
        match=match,
    )
    click.echo(f'Scripts on {client.id} at {client.scripts_dir}:')
    click.echo(str_res)


@scripts.command(short_help="Deploy a local script.")
@click.argument("path")
@click.option(
    "-r",
    "--rename",
    default=None,
    help="Name to give to deployed script. Defaults to script file name.",
)
@click.option(
    "-c",
    "--cmnd",
    default=None,
    help="Command string to write to script name",
)
@click.pass_context
def deploy(ctx, path, rename, cmnd):
    """
    Deploy Script

    Deploy a local script onto a TACC system.
    """
    client = ctx.obj["client"]
    local_file = None
    if rename is None:
        script_name = str(Path(path).stem)
    else:
        script_name = str(Path(rename).stem)
        local_file = path
    client.deploy_script(script_name, local_file=local_file, script_str=cmnd)
    str_res = _get_files_str(
        client,
        client.scripts_dir,
        attrs=['name', 'modified_time'],
        recurse=True,
        hidden=False,
        search='name',
        match=script_name,
    )
    click.echo(f'Deployed {script_name} on {client.id}:')
    click.echo(str_res)


@scripts.command(short_help="Run deployed script.")
@click.argument("script", type=str)
@click.option(
    "-j",
    "--job_id",
    default=None,
    show_default=True,
    help="If specified, job_id will be passed as first argument to script.",
)
@click.option(
    "-a",
    "--args",
    multiple=True,
    default=[],
    show_default=True,
    help="List of arguments to pass to script",
)
@click.option(
    "-l/-nl",
    "--logfile/--no-logfile",
    is_flag=True,
    default=False,
    show_default=True,
    help="Whether to re-direct stdout to a log file.",
)
@click.option(
    "-e/-ne",
    "--errfile/--no-errfile",
    is_flag=True,
    default=False,
    show_default=True,
    help="Whether to re-direct stderr to a err file.",
)
@click.pass_context
def run(ctx, script, job_id, args, logfile, errfile):
    """
    Run Deployed Script

    Run SCRIPT on JM_ID with passed in ARGS. SCRIPT must have been deployed
    already on JM_ID. Returns stdout. Take care with scripts that output a large
    amount of data. It is better practice for large output to be sent to a log
    file and for that to be downloaded seperately.
    """
    client = ctx.obj["client"]
    res = client.run_script(
        script,
        job_id=job_id,
        args=[],
        logfile=logfile,
        errfile=errfile,
        )
    click.echo(f"RUNNING: {client.user}@{client.id}$ " +
               f"{client.scripts_dir}/{script}")
    click.echo(filter_res([res], ["id", "status", "ts"]))
    click.echo(
        f"... use `taccjm process {res['id']} -w` to wait for "
        + "script to finish"
    )
