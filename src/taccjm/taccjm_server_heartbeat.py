import pdb
import time
import begin
import logging
import datetime
import numpy as np
from taccjm import taccjm_client as tc
from threading import Timer
from contextlib import contextmanager

global stats
stats = np.array([])
logger = logging.getLogger()


def get_stats():
    global stats
    msg = 'HEARTBEAT STATS\n'
    msg += '  Num calls = ' + str(len(stats)) + '\n'
    msg += '  Average time per call= ' + str(stats.mean()) + ' s\n'
    msg += '  Std Dev time = ' + str(stats.std()) + ' s\n'
    return msg


@contextmanager
def timing(label:str):
    t0 = time.perf_counter()
    yield lambda: (label, t1 - t0)
    t1 = time.perf_counter()


class RepeatingTimer(Timer):
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)


def heartbeat():
    try:
        global stats
        heartbeat_ts = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d_%H%M%S')
        logger.info('Heartbeat call at ' + heartbeat_ts)
        with timing('list_jm') as api_time:
            res = tc.list_jm()
        stats = np.append(stats, api_time()[1])
        logger.info('Succesful call with response ' + res.text)
        logger.info('    Timing [%s]: %.6f s' % api_time())
        logger.info(get_stats())
    except Exception as e:
        msg = 'Heartbeat failed to make list_jm call'
        logger.error(msg)


@begin.start(auto_convert=True)
def run(host: 'Host where server is running' = 'localhost',
        port: 'Port on which server is listening on' = '8221',
        hearbeat_interval: 'Time in minutes between heartbeats' = 5.0):
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
    # Turn on logging
    logging.basicConfig(level=logging.INFO)

    # Set endpoint for taccjm server
    tc.set_host(host=host, port=port)

    # Start heartbeat timer
    t = RepeatingTimer(hearbeat_interval*60.0, heartbeat)
    t.start()
