"""
Contains a base class for all services. All services must inherit
BaseService. If they wish to start in background, they should use the
BackgroundServiceStart mixin before BaseService while inheriting.
"""


import logging
import os
import subprocess
import sys
import threading

import psutil


logger = logging.getLogger(__name__)


class BaseService(object):
    """ Starts the service in the current thread. """
    def __init__(selfi, **kwargs):
        pass

    def service_start(self):
        self.before_service_start()
        return self.on_service_start()

    def service_stop(self, timeout=None):
        self.on_service_stop()

    def before_service_start(self):
        pass

    def on_service_start(self):
        pass

    def on_service_stop(self):
        pass

    def wait_for_start(self, timeout):
        pass

    def notify_start(self):
        pass


class BackgroundThreadServiceStart(object):
    """ Mixin with BaseServer to start in the background thread. """
    def service_start(self):
        self.before_service_start()
        self.service_thread = threading.Thread(target=self.on_service_start)
        self.service_thread.start()
        self.started_event = threading.Event()

    def service_stop(self, timeout=15):
        self.on_service_stop()
        self.service_thread.join()

    def wait_for_start(self, timeout):
        return self.started_event.wait(timeout)

    def notify_start(self):
        self.started_event.set()


class BasePlugin(BaseService):
    """ To be used by plugins loaded by WeaveServer (on the same machine)."""
    def __init__(self, **kwargs):
        self.venv_dir = kwargs.pop('venv_dir')
        self.auth_token = kwargs.pop('auth_token')
        super(BasePlugin, self).__init__(**kwargs)

    def service_start(self):
        self.started_event = threading.Event()
        self.child_thread = threading.Thread(target=self.child_process)
        self.child_thread.start()

    def service_stop(self, timeout=15):
        name = '.'.join(self.__module__.split('.')[:-1])
        logger.info("Stopping background process: %s", name)
        psutil.Process(self.service_pid).terminate()
        try:
            self.service_proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            psutil.Process(self.service_pid).kill()

    def child_process(self):
        name = '.'.join(self.__module__.split('.')[:-1])
        command = self.get_launch_command(name)
        self.service_proc = subprocess.Popen(command, env=os.environ.copy(),
                                             stdin=subprocess.PIPE,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.STDOUT)
        self.service_pid = self.service_proc.pid

        self.write_auth_token(self.serice_proc.stdin, self.get_auth_token())

        for line in iter(self.service_proc.stdout.readline, b''):
            content = line.strip().decode()
            if "SERVICE-STARTED-" + name in content:
                self.started_event.set()
            else:
                logger.info("[%s]: %s", name, content)

    def notify_start(self):
        name = '.'.join(self.__module__.split('.')[:-1])
        logger.info("SERVICE-STARTED-" + name)

    def wait_for_start(self, timeout):
        return self.started_event.wait(timeout)

    def get_launch_command(self, name):
        package_root = self.__module__.split('.')[0]
        py_file = sys.modules[package_root].__file__
        base_dir = os.path.dirname(os.path.dirname(py_file))
        return ["weave-launch", base_dir, self.venv_dir]

    def write_auth_token(self, file_handle, token):
        file_handle.write((token + "\n").encode())
        file_handle.flush()

    def get_auth_token(self):
        return self.auth_token


class MessagingEnabled(BaseService):
    def __init__(self, **kwargs):
        # Remember to keep MessagingService __init__ consistent.
        self.conn = kwargs.pop('conn')
        super(MessagingEnabled, self).__init__(**kwargs)

    def get_connection(self):
        return self.conn
