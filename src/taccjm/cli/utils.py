"""
TACCJM CLI Utils
"""
import re
import pdb
from datetime import datetime
from pathlib import Path
from posixpath import basename

from rich.table import Table
from rich.style import Style
from rich.theme import Theme
from rich.text import Text
from rich.highlighter import ISO8601Highlighter
from rich.console import Console
from rich.terminal_theme import MONOKAI

import taccjm.client.tacc_ssh_api as tacc_api
from taccjm.client.TACCClient import TACCClient

__author__ = "Carlos del-Castillo-Negrete"
__copyright__ = "Carlos del-Castillo-Negrete"
__license__ = "MIT"

DATE_HIGHLIGHT = ISO8601Highlighter()
console = Console()

bgcolor = "rgb(0,0,0)"
def_tab_style = Style(color="cyan", bold=True, bgcolor=bgcolor)
folder_style = Style(color="purple", italic=True, bgcolor=bgcolor)
file_style = Style(color="yellow", bold=True, bgcolor=bgcolor)
text_style = Style(color="white", bgcolor=bgcolor)
ts_style = Style(color="yellow", bgcolor=bgcolor)
num_style = Style(color="blue", bgcolor=bgcolor)
key_style = Style(color="cyan", bold=True, bgcolor=bgcolor)
err_style = Style(color="red", bold=True, bgcolor=bgcolor)
done_style = Style(color="green", bgcolor=bgcolor)
run_style = Style(color="yellow", bgcolor=bgcolor)

custom_theme = Theme({
    "key": "bold cyan",
    "info": "dim cyan",
    "warning": "magenta",
    "danger": "bold red"
})


def _get_client(conn_id, init=True):
    """
    Get first available client if conn_id is None, else client with conn_id.
    """
    if conn_id is None:
        connections = tacc_api.list_sessions()
        if len(connections) == 0:
            raise ValueError("No connections found")
        conn_id = connections[0]["id"]
    if init:
        return TACCClient(conn_id=conn_id)
    return conn_id


def _trim(st, max_len):
    if len(st) > max_len:
        return f'{st[-25:]} ...'
    else:
        return st


def _fmt_id(id_val, search=False, match=r'.'):
    """
    Go from id (int or string) to formatted output
    """
    search_str = str(id_val)
    if search and re.search(match, search_str) is None:
        return None
    return Text.assemble(search_str, style=key_style)


def _fmt_cmd(cmd, search=False, match=r'.'):
    """
    Format a command list/string
    """
    search_str = str(cmd)
    if search and re.search(match, search_str) is None:
        return None
    return Text.assemble(search_str, style=num_style)


def _fmt_ts(ts, fmt="%Y-%m-%d %H:%M:%S", search=False, match=r'.'):
    """
    Convert a float utc timestamp to a formatted output timestampe

    Parameters
    ----------
    ts: float
        UTC timestamp such as 1678828434.69
    """
    ts = ts if isinstance(ts, str) else datetime.utcfromtimestamp(
            ts).strftime(fmt)
    if search:
        # TODO implement search along ts column
        pass
    ts = Text.assemble(DATE_HIGHLIGHT(ts), style=ts_style)
    return ts


def _fmt_loglevel(loglevel, search=False, match=r'.'):
    """
    Go from loglevel string to formatted output
    """
    # TODO: implement level specific coloring
    if search:
        pass
    return Text.assemble(loglevel, style=err_style)


def _fmt_path(path, trash_dir=False, fnames=False, search=False, match=r'.'):
    """
    Go from path
    """
    # TODO: implement level specific coloring
    text = str(path)
    if trash_dir:
        text = basename(text).replace("___", "/")
    if fnames:
        text = basename(text)

    if search and re.search(match, text) is None:
        return None

    return Text.assemble(text, style=file_style)


def _fmt_trash(path, search=False, match=r'.'):
    return _fmt_path(path, trash_dir=True, fnames=True,
                     search=search, match=match)


def _fmt_permissions(ls_str, search=False, match=r'.'):
    """
    Go from ls_str to formatted permissions string
    """
    text = ls_str.split(' ')[0]

    if search and re.search(match, text) is None:
        return None

    return Text.assemble(text, style=file_style)


def _fmt_user(user, search=False, match=r'.'):
    """
    Go from user/uids to formatted output for table
    """
    # TODO: Convert int uid's to user string? Color according to user?
    text = str(user)

    if search and re.search(match, text) is None:
        return None

    return Text.assemble(text, style=key_style)


def _fmt_system(system, search=False, match=r'.'):
    """
    Go from system string to formatted output
    """
    text = str(system)

    if search and re.search(match, text) is None:
        return None

    # TODO: Color hostname and network path diff
    return Text.assemble(str(system), style=key_style)


def _fmt_bytes(nbytes, search=False, match=r'.'):
    """
    Go from system string to formatted output
    """
    # Convert bytes float into a human readable amount.
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    text = '%s %s' % (f, suffixes[i])

    if search and re.search(match, text) is None:
        return None

    # TODO: Color number and unit seperately
    return Text.assemble(text, style=key_style)


def _fmt_mode(mode, search=False, match=r'.'):
    """
    Go from system string to formatted output
    """
    # TODO: Do something with this
    text = str(mode)

    return Text.assemble(text, style=key_style)

def _fmt_status(status, search=False, match=r'.'):
    """
    Go from commmand status to formatted string
    """
    text = str(status)
    style = run_style
    if text == 'COMPLETE':
        style = done_style
    elif text == 'FAILED':
        style = err_style

    return Text.assemble(text, style=style)

def _fmt_error(error, search=False, match=r'.'):
    """
    Go from commmand stderr to formatted string
    """
    # TODO: Do something with this
    text = str(error)

    return Text.assemble(text, style=text_style)

def _fmt_output(output, search=False, match=r'.'):
    """
    Go from commmand stdout to formatted string
    """
    # TODO: Do something with this
    text = str(output)

    return Text.assemble(text, style=text_style)


def _fmt_rt(rt, search=False, match=r'.'):
    """
    Go from runtime float to human readable runtime formatted for rich.
    """
    # TODO: Do something with this
    text = str(rt)

    return Text.assemble(text, style=num_style)


def _fmt_cmnd_hist(history, search=False, match=r'.'):
    """
    Go from runtime float to human readable runtime formatted for rich.
    """
    # TODO: Do something with this
    text = str(history)

    return Text.assemble(text, style=num_style)


def build_table(rows, fields=None, max_n=int(1e6),
                search=None, match=r'.',
                fmts=None, output_file=None):
    """
    Build Rich Table

    Builds a table to print using rich console, applying appropriate formating
    and filtering rows based on given function.

    Parameters
    ----------
    rows: List[dict]
        List of strings defining rows of table.
    fields : List[string]
        List of strings containing names of fields to extract for each element.
    search : string, optional
        String containing column to perform string patter matching on to
        filter results.
    match : str, default='.'
        Regular expression to match strings in search column.
    max_n: int, default=1000000
        Regurn first max_n results, or last abs(max_n) results if < 0.
    styles: dict, default=None
        Style formatting to apply to table columns.
    output_file : str, optional
        Path to file to output result table to.

    """

    def _filter_fun(inp, fmts, fields=fields, search=search, match=match):
        """
        Go from raw dictionary row config to table row for rich table
        appropriatelyformatted. This function assumes the passed in search key
        is in the name of the same name type as the attrs dictionary
        """
        row = []
        for key in fields:
            text = fmts[key][1](inp[key], search=key == search, match=match)
            if text is None:
                row = None
                break
            else:
                row.append(text)

        return row

    def _def_fmt(inp):
        return Text.assemble(str(inp), style=def_tab_style)

    # Check args
    if fields is None or len(fields) == 0:
        if fmts is None:
            raise ValueError('both fields and fmts cant be None')
        fields = fmts.keys()
    if fmts is None:
        fmts = {(x, _def_fmt, _def_fmt) for x in fields}
    else:
        avail_keys = fmts.keys()
        bad_fields = [x for x in fields if x not in avail_keys]
        if len(bad_fields) > 0:
            raise ValueError(f'field and format key mismatch {bad_fields}')
    if search is not None and search not in fields:
        raise ValueError(f'search col {search} not in fields {fields}')

    # Build Table
    rows = [_filter_fun(x, fmts,
                        fields=fields,
                        search=search, match=match) for x in rows]
    rows = [x for x in rows if x is not None]
    if max_n > 0:
        # Get first max_n
        rows = rows[:max_n]
    else:
        # Get last max_n
        rows = rows[max_n:]
    table = Table(show_header=True, header_style="bold magenta")
    for field in fields:
        table.add_column(f"{fmts[field][0]} ({field})", style=fmts[field][2])
    for r in rows:
        table.add_row(*r)

    # Output to file if desired
    if output_file is not None:
        ext = Path(output_file).suffix
        if ext == 'svg':
            console.save_svg("example.svg", theme=MONOKAI)
        else:
            if len(ext) > 0 and ext != 'html':
                console.log('[red bold] Warning: ' +
                            'extension not recognized. Outputting to html')
            console.save_html(output_file, theme=MONOKAI)

        table = output_file

    return table


# Available return fields for sessions
_server_field_fmts = {
        "name": ("Server Process", _fmt_id, key_style),
        "pid": ("Process ID", _fmt_id, key_style),
        "started": ("Start TS", _fmt_ts, ts_style),
        "cmd": ("Command", _fmt_cmd, text_style),
}

_session_field_fmts = {
        "id": ("Connection ID", _fmt_id, key_style),
        "sys": ("System", _fmt_system, key_style),
        "user": ("User", _fmt_user, key_style,),
        "start": ("Started", _fmt_ts, ts_style),
        "last_ts": ("Last Accessed", _fmt_ts, ts_style),
        "home_dir": ("TACC Home Directory", _fmt_path, file_style),
        "work_dir": ("TACC Work Directory", _fmt_path, file_style),
        "scratch_dir": ("TACC Scratch Directory", _fmt_path, file_style),
}


# Available return fields per command, and their names in CLI table
_file_field_fmts = {
        "filename": ("Name", _fmt_path, file_style),
        "st_size": ("Size", _fmt_bytes, num_style),
        "ls_str": ("Permissions", _fmt_permissions, key_style),
        "st_uid": ("User", _fmt_user, key_style),
        "st_mtime": ("Modified Time", _fmt_ts, ts_style),
        "st_atime": ("Accessed Time", _fmt_ts, ts_style),
        "st_gid": ("Group", _fmt_user, key_style),
        "st_mode": ("Mode", _fmt_mode, key_style),
}

# When listing trash directory
_trash_field_fmts ={
        "filename": ("Name", _fmt_trash, file_style),
        "st_size": ("Size", _fmt_bytes, num_style),
        "st_mtime": ("Modified Time", _fmt_ts, ts_style),
}

# Available return fields for commands
_command_field_fmts = {
        "id": ("ID", _fmt_id, key_style),
        "key": ("Key", _fmt_id, key_style),
        "cmd": ("Command", _fmt_cmd, text_style),
        "status": ("Status", _fmt_status, done_style),
        "stdout": ("Output", _fmt_output, text_style),
        "stderr": ("Error", _fmt_error, text_style),
        "ts": ("Last Polled", _fmt_ts, ts_style),
        "rt": ("Approx Runtime", _fmt_rt, num_style),
        "rc": ("Return Code", _fmt_id, num_style),
        "history": ("Status History", _fmt_cmnd_hist, text_style),
}

# CLI Table Formats:
# - [+] TODO: Server
# - [+] TODO: Session
# - [ ] TODO: Files
# - [ ] TODO: Shell
# - [ ] TODO: Scripts
# - [ ] TODO: Jobs
# - [ ] TODO: Sims (Apps)

# TODO: Create format dictionaries for Queue table and Allocations Table
# _queue_fields = [
#     "job_id",
#     "job_name",
#     "username",
#     "state",
#     "nodes",
#     "remaining",
#     "start_time",
# ]
# _allocation_fields = ["name", "service_units", "exp_date"]


# def _get_files_str(
#     client,
#     path,
#     attrs=["Name", "Permissions", "Size", "User", "Date Modified"],
#     recurse=False,
#     hidden=False,
#     search="Name",
#     match=r".",
#     job_id=None,
#     trash_dir=False,
#     fnames=False,
#     max_n=100,
# ):
#     """
#     Utility function for generating string for listing files in a directory.
#     """
#     def _file_filt_fun(inp):
#         """
#         Format file dictioanry
#         """
#         row = []
#         for key in attrs:
#             if key == 'Name':
#                 text = inp['filename']
#                 if trash_dir:
#                     text = basename(text).replace("___", "/")
#                 elif fnames:
#                     text = basename(text)
#                 style = file_style
#             elif key == 'Permissions':
#                 # TODO: Exa-style coloring of permission:
#                 # See: https://the.exa.website/docs/colour-themes
#                 text = inp['ls_str'].split(' ')[0]
#                 style = text_style
#             elif key == 'Size':
#                 # TODO: Format colors better so: <num-color> <units-color>
#                 text = _size(inp['st_size'])
#                 style = num_style
#             elif key == 'Date Modified':
#                 text = DATE_HIGHLIGHT(
#                         datetime.utcfromtimestamp(
#                             inp["st_mtime"]).strftime("%Y-%m-%d %H:%M:%S")
#                         )
#                 style = ts_style
#             elif key == 'User':
#                 # TODO: Determine user from uid
#                 text = str(inp['st_uid'])
#                 style = text_style
#             try:
#                 row.append(Text.assemble(text, style=style))
#             except Exception:
#                 console.print_exception(show_locals=True)
#                 pdb.set_trace()
# 
#             if key == search and re.search(match, text) is None:
#                 row = None
#                 break
# 
#         return row
# 
#     res = client.list_files(
#         path=path,
#         recurse=recurse,
#         hidden=hidden,
#         job_id=job_id,
#         local=False,
#     )
# 
#     rows = [_file_filt_fun(x) for x in res]
#     rows = [x for x in rows if x is not None]
# 
#     table = Table(show_header=True, header_style="bold magenta")
#     table.title = ("[not italic magenta bold]:desktop_computer:" +
#                    f"  {client.id}  :open_file_folder: {path}[/]")
#     for field in attrs:
#         table.add_column(field, style=_file_field_fmts[field])
#     for r in rows:
#         table.add_row(*r)
# 
#     return table
# 
# 

# def _get_commands_table(
#     client,
#     attrs=_command_fields,
#     search="id",
#     match=r".",
#     max_str_len=25,
#     max_n=-25,
# ):
#     if "id" not in _command_fields:
#         raise ValueError(f'Search col {search} must be in {_command_fields}')
# 
#     def _command_filt_fun(inp):
#         """
#         Go from raw command config to table row and format
#         """
#         row = None
#         if re.search(match, str(inp[search])) is not None:
#             row = []
#             row.append(Text.assemble(str(inp.pop(search)), style=key_style))
#             for key in attrs:
#                 if key in ['stdout', 'stdin', 'cmd']:
#                     row.append(Text.assemble(_trim(inp[key], max_str_len),
#                                style=text_style))
#                 elif key == 'rt':
#                     row.append(Text.assemble(str(inp[key]), style=num_style))
#                 elif key == 'rc':
#                     row.append(Text.assemble(
#                         str(inp[key]),
#                         style=err_style if inp[key] != 0 else done_style))
#                 elif key == 'status':
#                     style = run_style
#                     if inp[key] == 'COMPLETE':
#                         style = done_style
#                     elif inp[key] == 'FAILED':
#                         style = err_style
#                     row.append(Text.assemble(str(inp[key]), style=style))
#                 elif key == 'ts':
#                     # TODO: Format ts better?
#                     row.append(Text.assemble(
#                         DATE_HIGHLIGHT(inp[key]), style=ts_style))
# 
#         return row
# 
#     res = tacc_api.list_commands(client.id)
# 
#     table = build_table(res, attrs, filter_fun=_command_filt_fun,
#                         max_n=max_n, styles=_command_field_fmts)
#     table.title = ("[not italic magenta bold]:desktop_computer: Commands")
#     table.caption = ("[not italic cyan] Showing " +
#                      f"{'last' if max_n < 0 else 'first'} {abs(max_n)}")
# 
#     return table
