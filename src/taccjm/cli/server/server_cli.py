"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import pdb
import time
from rich.console import Console
from rich.syntax import Syntax
from rich.panel import Panel
from rich.table import Table
from rich.traceback import install
import rich_click as click
from taccjm.cli.utils import build_table, _server_field_fmts, _log_field_fmts
from taccjm.client.tacc_ssh_api import find_server, get_logs
from rich import print

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

install(suppress=[click], max_frames=3)
CONSOLE = Console()


@click.group(short_help="find/log")
@click.pass_context
def server(ctx, cols=None, search=None, match=r'.'):
    """TACC Job Manager Server Operations

    Commands to manage the server that maintains TACC ssh clients.
    """
    ctx.ensure_object(dict)


@server.command(short_help="Locate TACCJM server")
@click.option("-s/-ns", "--start/--no-start",
              is_flag=True, default=False, show_default=True,
              help='If set, will start the server if it is not alive. If ' +
              'kill is set as well, the server will effectively be restarted.')
@click.option("-k/-nk", "--kill/--no-kill",
              is_flag=True, default=False, show_default=True,
              help='If set, will kill the server it is alive. This preceeds' +
              'starting the server.')
@click.pass_context
def find(ctx, kill, start):
    """
    Find Server

    Locate (host,port) where server is running. Can kill/start server as
    needed with appropriate flags. Note that will only show server for the
    default taccjm (host, port) being used. To change (host, port) combo, use
    `taccjm --server <host> <port> server find` with appropriate host and port.
    """
    res = find_server(start, kill)
    rows = []
    for x in res.keys():
        ts = time.time()
        cmd = []
        if 'create_time' in dir(res[x]):
            ts = res[x].create_time()
        if 'info' in dir(res[x]):
            cmd = res[x].info['cmdline']
        rows.append({
            "name": 'Server' if x == 'server' else 'Heartbeat',
            "pid": res[x].pid,
            "started": ts,
            "cmd": cmd,
        })

    # Default set of columns for this operation
    cols = ["name", "pid", "started"]
    if ctx.obj['cols'] != ():
        cols = ctx.obj['cols']
    table = build_table(
            rows,
            fields=cols,
            fmts=_server_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'])
    table.title = f"[not italic] Servers on {ctx.obj['host']}:{ctx.obj['port']}"

    CONSOLE.print(table)


@server.command(short_help="Read TACCJM Logs")
@click.option("-m", "max_len",
              default=1000000, show_default=True,
              help='Max length of log string to retrive (from end of file)')
@click.pass_context
def logs(ctx, max_len):
    """
    Read server logs
    """
    logs = get_logs(max_len)
    cols = None
    if ctx.obj['cols'] != ():
        cols = ctx.obj['cols']
    table = build_table(
            logs['sv_log']['contents'],
            fields=cols,
            fmts=_log_field_fmts,
            search=ctx.obj['search'], match=ctx.obj['match'])
    table.title = f"[not italic] Server {ctx.obj['host']}:{ctx.obj['port']} Logs"

    CONSOLE.print(table)
