"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import pdb
import rich_click as click

from taccjm.client import tacc_ssh_api as tacc_api
from taccjm.cli.utils import build_table, _get_client, _command_field_fmts

from rich.console import Console
CONSOLE = Console()

# from .apps import app_commands as apps_cli
# from .jobs import job_commands as jobs_cli
# from .scripts import script_commands as scripts_cli

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

# TODO: Change 'key' to 'user' to see user making request
# TODO: Change output of commands
#   - Move stdout/stderr to panels
#   - Remove status histroy -> to separate table with panel (optional to output)


@click.group(short_help="exec/process/list")
@click.pass_context
def commands(ctx):
    """
    TACC Job Manager Shell
    """
    pass


@commands.command(short_help="Execute command on system via ssh")
@click.argument("command")
@click.option(
    "-w/-nw",
    "--wait/--no-wait",
    is_flag=True,
    default=True,
    help="Wait for command to execute",
)
@click.option(
    "-k",
    "--key",
    default=None,
    help="Key to tag command with.",
)
@click.option(
    "-f/-nf",
    "--fail/--no-fail",
    default=False,
    is_flag=True,
    help="Whether command should generate an exception on failure.",
)
@click.pass_context
def exec(ctx, command, wait=False, fail=True, key='CLI'):
    """
    Execute an arbitrary command via ssh.
    """
    client = _get_client(ctx.obj['session_id'])
    res = client.exec(command, wait=wait, fail=fail, key=key, local=False)
    str_command = command[:20] + "..." if len(command) > 20 else command
    table = build_table(
            [res],
            fields=ctx.obj['cols'],
            fmts=_command_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'],
            max_n=ctx.obj['max_n'])
    if wait:
        table.title = (f"[not italic] {client.user}@{client.id}$ " + 
                       f"{str_command}:\n{res['stdout']}")
    else:
        table.title = (f"[not italic] Use `taccjm process {res['id']} -w` to wait for " +
                       "wommand to finish")
    CONSOLE.print(table)


@commands.command(short_help="Process a command that was executed via ssh")
@click.option(
    "-i",
    "--command_id",
    default=None,
    help="ID of command to process. If None, all active will be polled.",
    show_default=True,
)
@click.option(
    "-p/-np",
    "--poll/--no-poll",
    is_flag=True,
    default=True,
    help="Poll the command to update its status.",
    show_default=True,
)
@click.option(
    "-w/-nw",
    "--wait/--no-wait",
    is_flag=True,
    default=False,
    help="Wait for command to execute",
    show_default=True,
)
@click.option(
    "-e/-ne",
    "--error/--no-error",
    is_flag=True,
    default=True,
    help="Throw error if encountered.",
    show_default=True,
)
@click.option(
    "-n",
    "--nbytes",
    default=0,
    show_default=True,
    help=("Number of bytes to pull from stdout. Note will hang until nbytes " +
          "become available")
)
@click.pass_context
def get(ctx, command_id=None, wait=False, error=True, nbytes=None, poll=True):
    """
    Poll/Wait for a command that was executed via SSH.
    """
    client = _get_client(ctx.obj['session_id'])
    res = client.process(command_id, poll=poll,
                         wait=wait, error=error, nbytes=nbytes)
    table = build_table(
            res,
            fields=ctx.obj['cols'],
            fmts=_command_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'],
            max_n=ctx.obj['max_n'])
    if wait:
        table.title = ("[not italic magenta bold]:desktop_computer:" +
                       "  Commands :desktop_computer:[/]")
        # TODO: Print outputs into panels nicely?
        # CONSOLE.print(f"{client.user}@{client.id}$ {str_c}" +
        #               f"\n{res['stdout']}")
    else:
        table.title = (
            f"[not italic] Use [red bold]taccjm process <ID> -w[/] " +
            f"to wait for command to finish"
        )
    CONSOLE.print(table)


@commands.command(short_help="List commands.")
@click.pass_context
def list(ctx):
    """
    List commands

    List files in a given directory (defaults to home). Can search using
    regular expressions on any given output attribute.
    """
    session_id = _get_client(ctx.obj['session_id'], init=False)
    table = build_table(
            tacc_api.list_commands(session_id),
            fields=ctx.obj['cols'],
            fmts=_command_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'],
            max_n=ctx.obj['max_n'])
    table.title = ("[not italic magenta bold]:desktop_computer:" +
                   "  Commands :desktop_computer:[/]")
    CONSOLE.print(table)
