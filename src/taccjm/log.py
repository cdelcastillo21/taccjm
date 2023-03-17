"""
TACCJobManager Log functionality


"""
from loguru import logger
from rich.logging import RichHandler
from rich.console import Console

logger.disable('taccjm')

def enable(file=None,
           level='INFO', fmt="{message}"):
    """
    Turn on logging for module with appropriate message format
    """
    if file is None:
        sink = RichHandler(markup=True, rich_tracebacks=True)
    else:
        sink = RichHandler(console=Console(file=file),
                           markup=True,
                           rich_tracebacks=True)
    logger.configure(handlers=[{"sink": sink, "level":level, "format": fmt}])
    logger.enable('taccjm')
    logger.info('Logger initialized')

    return logger


def disable():
    """
    Turn of logging
    """
    logger.disable('taccjm')
# 
