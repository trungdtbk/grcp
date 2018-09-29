"""
Main programm
"""
import sys, time
import eventlet
eventlet.monkey_patch()

from grcp.cfg import CONF

from grcp.base.app_manager import AppManager
from grcp.utils import get_logger

logger = get_logger('grcp', loglevel='info')

def main():
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
