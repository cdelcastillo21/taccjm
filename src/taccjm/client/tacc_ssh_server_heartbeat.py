import logging
import pdb
import time
from contextlib import contextmanager
from pathlib import Path
from threading import Timer

import begin
import numpy as np

from taccjm.client import tacc_ssh_api as tsa
from taccjm.constants import TACCJM_DIR
from taccjm.utils import get_log_level, parse_allocations_string
from taccjm.log import enable

global stats
stats = np.array([])


def get_stats():
    global stats
    num_calls = len(stats)
    avg_time = stats.mean()
    std_dev = stats.std()
    msg = f"HEARTBEAT STATS\n\rNum calls = {num_calls}\n\t"
    msg += f"Average time per call= {avg_time}s\n\tStd Dev time = {std_dev}s\n"
    _logger.info(
        msg, extra={"num_calls": num_calls, "avg_time": avg_time, "std_dev": std_dev}
    )


@contextmanager
def timing(label: str):
    t0 = time.perf_counter()
    yield lambda: (label, t1 - t0)
    t1 = time.perf_counter()


class RepeatingTimer(Timer):
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)


def poll_session(session_id: str) -> None:
    """
    Poll session by:

        1. Sending a command (checking allocations on system) to the shell
        session, therefore keeping it alive. 
        2. Poll all active commands to update there status server side.

    Parameters
    ----------
    session_id : str
        ID of session to poll
    """
    alloc_command = tsa.exec(session["id"], "/usr/local/etc/taccinfo",
                 key='HEARTBEAT', wait=False, fail=False)
    _logger.info(
        f"Polling active processes for session {session['id']}",
        extra={"session": session},
    )
    active = tsa.process(session["id"], poll=True, wait=False)
    _logger.info(
        f"Found {len(active)} commands on {session_id}.",
        extra={"session_id": session_id, "active": active},
    )
    # See if alloc_command still amongst active commands
    cmnd = [c for c in active if alloc_command['id'] == c['id']]
    if len(cmnd) != 0:
        cmnd = [tsa.process(session["id"], cmnd_id=alloc_command['id'],
                           poll=True, wait=True)]
    allocs = parse_allocations_string(cmnd[0]["stdout"])
    _logger.info(
            f"Found {len(allocs)} allocations.",
            extra=[allocs],
    )


def heartbeat():
    try:
        global stats
        _logger.info("Starting heartbeat list_sessions() call")
        with timing("list_sessions()") as api_time:
            res = tsa.list_sessions()
            # TODO: Poll active commands on heartbeat for each session
            # TODO: Dail (hourly?) update allocation stats
            # for session in res:
            #     pull_allocatons(session)
        stats = np.append(stats, api_time()[1])
        _logger.info(
            "%s Done in %.6f s" % api_time(),
            extra={"api_time": api_time()[1], "connections": res},
        )
        get_stats()
    except Exception as e:
        msg = f"Heartbeat failed to make list_session call {e}"
        _logger.error(msg)


@begin.start(auto_convert=True)
def run(
    host: "Host where server is running" = "localhost",
    port: "Port on which server is listening on" = "8221",
    heartbeat_interval: "Time in minutes between heartbeats" = 0.5,
    loglevel: "Level for logging" = "INFO",
):
    """
    Run

    Main entry-point for heart-beat script. Note begin decorator makes function
    arguments command line arguments accepted to run this as a main entry point.
    Note function does not return, but runs continuously until terminated.

    Parameters
    ----------
    host : str, default='localhost'
        Host where taccjm server is running.
    port : int, default=8221
        Port that taccjm server is listening on.

    Returns
    -------

    """
    global _logger

    # LOGFILE = f"{TACCJM_DIR}/ssh_server_{host}_{port}/heartbeat/log.txt"
    _logger = enable(level=loglevel)

    # Set endpoint for taccjm server
    _logger.info(
        f"Setting host and port to ({host}, {port})",
        extra={"host": host, "port": port}
    )
    tsa.set_host(host=host, port=port)

    # Start heartbeat timer
    h_int = heartbeat_interval * 60.0
    _logger.info(
        f"Starting heartbeat every {h_int}s = {heartbeat_interval}m",
        extra={"host": host, "port": port},
    )
    t = RepeatingTimer(h_int, heartbeat)
    t.start()
