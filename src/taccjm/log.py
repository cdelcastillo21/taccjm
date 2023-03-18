"""
TACCJobManager Log functionality


"""
from loguru import logger
from rich.logging import RichHandler


logger.disable('taccjm')


def enable(file=None,
           level='INFO', fmt="{message}"):
    """
    Turn on logging for module with appropriate message format
    """
    if file is None:
        logger.configure(handlers=[
            {"sink": RichHandler(markup=True, rich_tracebacks=True),
             "level": level, "format": fmt}])
    else:
        logger.configure(handlers=[
            {"sink": file, "serialize": True,
             "level": level, "format": fmt, "rotation": "10 MB",
             "enqueue": True}])
    logger.enable('taccjm')
    logger.info('Logger initialized')

    return logger


def disable():
    """
    Turn of logging
    """
    logger.disable('taccjm')
# 
