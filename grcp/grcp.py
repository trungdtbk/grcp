#!/usr/bin/env python
"""
Main programm
"""
import os
import sys
import time

import grcp

from .cfg import CONF
from .utils import get_logger
from .app_manager import AppManager

loglevel = os.environ.get('GRCP_LOG_LEVEL', 'info')
logger = get_logger('grcp', logfile=os.environ.get('GRCP_LOG', 'STDOUT'), loglevel=loglevel)

def main():
    logger.info('starting the controller')
    CONF(sys.argv[1:])
    app_list = ['grcp.core.topology']
    app_list = app_list + CONF.app_list + CONF.app
    app_manager = AppManager.get_instance()
    app_manager.load_apps(app_list)
    threads = app_manager.instantiate_apps()
    for t in threads:
        t.wait()
    logger.info('shutting down the controller')
    app_manager.close()

if __name__ == '__main__':
    sys.exit(main())
