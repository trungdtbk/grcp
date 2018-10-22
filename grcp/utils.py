import logging
from logging.handlers import WatchedFileHandler
import sys

def get_logger(logname, logfile='STDOUT', loglevel='info'):
    stream_handlers = {
            'STDOUT': sys.stdout,
            'STDERR': sys.stderr,
            }

    if logfile in stream_handlers:
        log_handler = logging.StreamHandler(stream_handlers[logfile])
    else:
        log_handler = WatchedFileHandler(logfile)
    log_fmt = '%(asctime)s %(name)-6s %(levelname)-8s %(message)s'
    log_handler.setFormatter(
        logging.Formatter(log_fmt, '%b %d %H:%M:%S'))
    logger = logging.getLogger(logname)
    logger.addHandler(log_handler)
    logger.setLevel(loglevel.upper())
    return logger

