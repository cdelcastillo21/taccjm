"""
TACCJM Scripts CLI
"""
from pathlib import Path

import rich_click as click
from rich.traceback import install
from rich.console import Console

from taccjm.cli.utils import _get_client, _file_field_fmts, \
        _command_field_fmts, build_table


install(suppress=[click], max_frames=3)
CONSOLE = Console()

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


@click.group(short_help="list/deploy/run")
@click.pass_context
def scripts(ctx):
    """
    TACC Job Manager Scripts

    CLI Entrypoint for commands related to scripts.
    """
    ctx.ensure_object(dict)
    ctx.obj['client'] = _get_client(ctx.obj['session_id'])


@scripts.command(short_help="List deployed scripts.")
@click.pass_context
def list(ctx):
    """
    List scripts deployed on JM_ID
    """
    client = ctx.obj["client"]
    rows = client.list_files(
        path=client.scripts_dir,
        recurse=True,
        hidden=False,
        local=False,
    )
    table = build_table(
            rows,
            fields=ctx.obj['cols'],
            fmts=_file_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'])

    CONSOLE.print(table)


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
@click.option(
    "-r/-nr",
    "--run/--no-run",
    default=False,
    show_default=True,
    help="Whether to run the script right after deploying.",
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
def deploy(ctx, path, rename=None, cmnd=None,
           run=False, logfile=False, errfile=False):
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

    if run:
        CONSOLE.print(f'Script {script_name} deployed. Running...')
        res = client.run_script(
            script_name,
            args=[],
            logfile=logfile,
            errfile=errfile,
            )
        table = build_table(
                [res],
                fields=ctx.obj['cols'],
                fmts=_command_field_fmts)
        table.title= (
            f"[not italic] Use [red]taccjm commands get -i {res['id']} -w[/] "
            f"to wait for script to finish"
        )
    else:
        rows = client.list_files(
            path=client.scripts_dir,
            recurse=True,
            hidden=False,
            local=False,
        )
        table = build_table(
                rows,
                fields=ctx.obj['cols'],
                fmts=_file_field_fmts,
                search='filename', match=script_name)
        table.title = f'[not italic] Deployed {script_name} on {client.id}:'
    CONSOLE.print(table)


@scripts.command(short_help="Run deployed script.")
@click.argument("script", type=str)
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
def run(ctx, script, args=[], logfile=False, errfile=False):
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
        args=[],
        logfile=logfile,
        errfile=errfile,
        )
    table = build_table(
            [res],
            fields=ctx.obj['cols'],
            fmts=_command_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'],
            max_n=ctx.obj['max_n'])
    table.title = (f"[not italic] Running {script}")
    table.caption = (
        f"[not italic] Use [red]taccjm commands get -i {res['id']} -w[/] "
        f"to wait for script to finish"
    )
    CONSOLE.print(table)
