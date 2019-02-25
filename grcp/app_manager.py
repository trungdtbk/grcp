"""
Application management framework (inspired by Ryu application framework).
New application to be created by inherit the class AppBase
"""
import eventlet
eventlet.monkey_patch(socket=True, time=True)

import importlib
import inspect
import logging
import traceback

import greenlet

logger = logging.getLogger('grcp')

def spawn(*args, **kwargs):
    # taken from ryu.lib.hub
    def _launch(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except greenlet.GreenletExit:
            pass
        except BaseException as e:
            print('hub: uncaught exception: %s', traceback.format_exc())

    return eventlet.spawn(_launch, *args, **kwargs)

def listen_to_ev(ev_classes):
    def set_ev_handler(handler):
        if not hasattr(handler, 'events_to_listen'):
            handler.events_to_listen = ev_classes
        return handler
    return set_ev_handler

def register_handlers(app):
    for name, method in inspect.getmembers(app, inspect.ismethod):
        if hasattr(method, 'events_to_listen'):
            for ev_cls in method.events_to_listen:
                app.register_handler(ev_cls, method)

class AppBase(object):
    """Base class for applications. All applications on sdRCP must be subclass of AppBase
    """

    def __init__(self):
        self.name = self.__class__.__name__
        self.threads = []
        self.handlers = {}
        self.observers = {}
        self.events = eventlet.Queue(512)
        self.main_thread = None
        self.running = True

    def register_handler(self, ev_cls, handler):
        self.handlers.setdefault(ev_cls, set())
        self.handlers[ev_cls].add(handler)

    def register_observers(self, apps):
        for app in apps:
            if app.name == self.name:
                continue
            for ev_cls in app.handlers.keys():
                self.observers.setdefault(ev_cls, set())
                self.observers[ev_cls].add(app)
                logger.info('registered observer %s for event %s' % (app.name, ev_cls.__name__))

    def _get_handlers(self, event):
        name = event.__class__
        if name in self.handlers:
            return self.handlers[name]
        return []

    def _event_loop(self):
        while self.running:
            try:
                event = self.events.get()
                handlers = self._get_handlers(event)
                for handler in handlers:
                    handler(event)
            except Exception as e:
                raise Exception(e)

    def _get_observers(self, ev):
        if ev.__class__ in self.observers:
            return self.observers[ev.__class__]
        return []

    def _receive_event(self, ev):
        self.events.put_nowait(ev)

    def send_event_to_observers(self, ev):
        for observer in self._get_observers(ev):
            observer._receive_event(ev)

    def _main_loop(self):
        return

    def start(self):
        t = spawn(self._event_loop)
        self.threads.append(t)
        t = spawn(self._main_loop)
        return t

    def stop(self):
        logger.info('closing down app: %s' % self.name)
        self.running = False


def get_topo_manager():
    app_manager = AppManager.get_instance()
    if 'topo_manager' in app_manager.applications:
        return app_manager.applications['topo_manager']
    return None

class AppManager(object):

    _instance = None

    def __init__(self):
        self.applications = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = AppManager()
        return cls._instance

    def load_app(self, app_name):
        mod = importlib.import_module(app_name)
        app_clses = inspect.getmembers(mod, lambda cls: (inspect.isclass(cls) and issubclass(cls, AppBase)))
        for app_cls_name, app_cls in app_clses:
            if 'AppBase' not in app_cls_name:
                return app_cls
        return None

    def load_apps(self, app_list):
        for name in app_list:
            logger.info('loading app %s' % name)
            app_cls = self.load_app(name)
            if app_cls is None:
                logger.error('failed to load app %s' % name)
                continue
            app = app_cls() # initialize an instance of app class
            self.applications[app.name] = app
            register_handlers(app)
        for name, app in self.applications.items():
            app.register_observers(self.applications.values())

    def instantiate_apps(self):
        threads = []
        for app in self.applications.values():
            threads.append(app.start())
        return threads

    def close(self):
        for app in self.applications.values():
            app.stop()
