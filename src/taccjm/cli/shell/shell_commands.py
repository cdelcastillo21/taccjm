"""
TACCJM CLI

CLI for using TACCJM Client.
"""
import rich_click as click

from taccjm.cli.utils import build_table, _get_client, _command_fields, \
        _get_commands_table

from rich.console import Console
CONSOLE = Console()

# from .apps import app_commands as apps_cli
# from .jobs import job_commands as jobs_cli
# from .scripts import script_commands as scripts_cli

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"


@click.group(short_help="exec/process/list")
@click.option(
    "-c",
    "--conn_id",
    default=None,
    help="Connection to execute operation on. Defaults to first available.",
)
@click.pass_context
def shell(ctx, conn_id):
    """
    TACC Job Manager Shell
    """
    client = _get_client(conn_id)
    ctx.ensure_object(dict)
    ctx.obj["client"] = client


@shell.command(short_help="Execute command on system via ssh")
@click.argument("command")
@click.option(
    "-w/-nw",
    "--wait/--no-wait",
    is_flag=True,
    default=True,
    help="Wait for command to execute",
)
@click.option(
    "-e/-ne",
    "--error/--no-error",
    is_flag=True,
    default=True,
    help="Throw error if encountered.",
)
@click.pass_context
def exec(ctx, command, wait, error):
    """
    Execute an arbitrary command via ssh.
    """
    client = ctx.obj['client']
    res = client.exec(command, wait, error)
    str_command = command[:20] + "..." if len(command) > 20 else command
    if wait:
        CONSOLE.print(build_table([res], ["id", "status", "rt"]))
        CONSOLE.print(f"{client.user}@{client.id}$ {str_command}" + f"\n{res['stdout']}")
    else:
        CONSOLE.print(build_table([res], ["id", "status", "ts"]))
        CONSOLE.print(
            f"Use `taccjm process {res['id']} -w` to wait for command " + "to finish"
        )


@shell.command(short_help="Process a command that was executed via ssh")
@click.argument("command_id")
@click.option(
    "-w/-nw",
    "--wait/--no-wait",
    is_flag=True,
    default=True,
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
    "-nbytes", default=0, show_default=True, help="Run comand locally or remotely"
)
@click.pass_context
def process(ctx, command_id, wait, error, nbytes):
    """
    Poll/Wait for a command that was executed via SSH.
    """
    max_cmnd_str_len = 50
    client = ctx.obj['client']
    res = client.process(command_id, wait, error, nbytes)
    str_c = res["cmd"][:max_cmnd_str_len] + "..." if len(
            res["cmd"]) > max_cmnd_str_len else res["cmd"]
    if wait:
        CONSOLE.print(build_table([res], ["id", "status", "rt"]))
        CONSOLE.print(f"{client.user}@{client.id}$ {str_c}" + f"\n{res['stdout']}")
    else:
        CONSOLE.print(build_table([res], ["id", "status", "ts"]))
        CONSOLE.print(f"{client.user}@{client.id}$ {str_c}")
        CONSOLE.print(
            f"... use `taccjm process {res['id']} -w` to wait for "
            + "command to finish"
        )


@shell.command(short_help="List commands.")
@click.option(
    "--attrs",
    type=click.Choice(_command_fields, case_sensitive=False),
    multiple=True,
    default=["id", "cmd", "status", "ts", "rt"],
    help="Command info to include in output.",
    show_default=True,
)
@click.option(
    "--search",
    type=click.Choice(_command_fields, case_sensitive=False),
    default="id",
    help="Column to search.",
    show_default=True,
)
@click.option(
    "-m",
    "--match",
    default=r".",
    show_default=True,
    help="Regular expression to match.",
)
@click.pass_context
def list(ctx,  attrs, search, match):
    """
    List Files

    List files in a given directory (defaults to home). Can search using
    regular expressions on any given output attribute.
    """
    client = ctx.obj["client"]
    table = _get_commands_table(
        client,
        attrs=attrs,
        search=search,
        match=match,
    )
    table.title = ("[not italic magenta bold]:desktop_computer:" +
                   f"  {client.id}  Commands[/]")
    CONSOLE.print(table)
