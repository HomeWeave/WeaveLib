"""
Contains a base class for all services. All services must inherit
BaseService. If they wish to start in background, they should use the
BackgroundServiceStart mixin before BaseService while inheriting.
"""


import json
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
    """ Mixin with BaseService to start in the background thread. """
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


class BackgroundProcessServiceStart(object):
    """ Mixin with BaseService to start in another subprocess. """
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
        self.service_proc = subprocess.Popen(["weave-launch"],
                                             stdin=subprocess.PIPE,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.STDOUT)

        self.service_pid = self.service_proc.pid

        params = json.dumps(self.get_params())
        self.service_proc.stdin.write((params + "\n").encode())

        lines = iter(self.service_proc.stdout.readline, b'')
        for line in lines:
            content = line.strip().decode()
            if "SERVICE-STARTED-" + name in content:
                self.started_event.set()
                break
            else:
                logger.info("[%s]: %s", name, content)

        # Now that the service start line has been found, just echo all lines.
        for line in lines:
            content = line.strip().decode()
            logger.info("[%s]: %s", name, content)

    def notify_start(self):
        name = '.'.join(self.__module__.split('.')[:-1])
        logger.info("SERVICE-STARTED-" + name)

    def wait_for_start(self, timeout):
        return self.started_event.wait(timeout)

    def get_params(self):
        return {}


class AuthenticatedPlugin(BaseService):
    """ Probably should not be directly used. This is purely provide the
        get_auth_token() interface for DummyMessagingService class in core.
    """
    def __init__(self, **kwargs):
        self.auth_token = kwargs.pop('auth_token')
        super(AuthenticatedPlugin, self).__init__(**kwargs)

    def get_auth_token(self):
        return self.auth_token


class BasePlugin(BackgroundProcessServiceStart, AuthenticatedPlugin):
    """ To be used by plugins loaded by WeaveServer (on the same machine)."""
    def __init__(self, **kwargs):
        self.venv_dir = kwargs.pop('venv_dir')
        self.plugin_dir = kwargs.pop('plugin_dir')
        self.ignore_hierarchy = kwargs.pop('ignore_hierarchy', False)
        super(BasePlugin, self).__init__(**kwargs)

    def get_params(self):
        params = {
            "venv_dir": self.venv_dir,
            "auth_token": self.auth_token,
            "plugin_dir": self.plugin_dir
        }
        if self.ignore_hierarchy:
            params["ignore_hierarchy"] = True
        return params


class MessagingEnabled(AuthenticatedPlugin):
    def __init__(self, **kwargs):
        # Remember to keep MessagingService __init__ consistent.
        self.conn = kwargs.pop('conn')
        super(MessagingEnabled, self).__init__(**kwargs)

    def get_connection(self):
        return self.conn
