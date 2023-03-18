"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import pdb
import os

from rich.console import Console
from rich.traceback import install
import rich_click as click
from rich_click.cli import patch

patch()

from taccjm.client.tacc_ssh_api import set_host
from taccjm.constants import TACC_SSH_HOST, TACC_SSH_PORT
from taccjm.cli.server import server_cli
from taccjm.cli.sessions import sessions_cli
from taccjm.cli.files import files_cli
from taccjm.cli.commands import commands_cli
from taccjm.cli.scripts import scripts_cli
# from taccjm.cli.jobs import job_commands as jobs_cli
# from taccjm.cli.utils import _get_client, build_table

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

install(suppress=[click], max_frames=3, show_locals=True)
CONSOLE = Console()

TACC_PW = os.getenv("TACC_PW")


@click.group(short_help="TACC Job Manager CLI")
@click.option(
    "--server",
    type=(str, int),
    default=(TACC_SSH_HOST, TACC_SSH_PORT),
    help="Host and port of location of TACC JM server to talk to.",
)
@click.option(
    "-s",
    "--session_id",
    default=None,
    help="Session to execute operation on. Defaults to first available.",
)
@click.option(
    "-c",
    "--cols",
    multiple=True,
    default=None,
    help="Output columns to display.",
    show_default=True,
)
@click.option(
    "-k",
    "--search",
    default=None,
    help="Column to filter results on.",
)
@click.option(
    "-m",
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to match on result column.",
)
@click.option(
    "-n",
    "--max_n",
    default=100,
    show_default=True,
    help="Maximum number of entries to show from top. Use <0 for bottom.",
)
@click.pass_context
def cli(ctx, server=(TACC_SSH_HOST, TACC_SSH_PORT),
        session_id=None, cols=None, search=None, match=r'.', max_n=100):
    """
    TODO: Add explanation here
    """
    ctx.ensure_object(dict)
    if server[0] != TACC_SSH_HOST or server[1] != TACC_SSH_PORT:
        ctx.obj['host'], ctx.obj['port'] = set_host(server[0], server[1])
    else:
        ctx.obj['host'], ctx.obj['port'] = TACC_SSH_HOST, TACC_SSH_PORT
    ctx.obj["session_id"] = session_id
    ctx.obj["cols"] = cols
    ctx.obj["search"] = search
    ctx.obj["match"] = match
    ctx.obj["max_n"] = max_n


cli.add_command(server_cli.server)
cli.add_command(sessions_cli.sessions)
cli.add_command(files_cli.files)
cli.add_command(commands_cli.commands)
cli.add_command(scripts_cli.scripts)
# cli.add_command(jobs_cli.jobs)
# cli.add_command(shell_cli.shell)
