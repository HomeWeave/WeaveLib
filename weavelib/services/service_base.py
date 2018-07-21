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
from contextlib import suppress

import psutil

from weavelib.rpc import RPCClient


logger = logging.getLogger(__name__)


def get_root_rpc_client(token):
    root_rpc_info = {
        "name": "",
        "description": "",
        "apis": {
            "register_rpc": {
                "name": "register_rpc",
                "description": "",
                "args": [
                    {
                        "name": "name",
                        "description": "Name of RPC",
                        "schema": {"type": "string"}
                    },
                    {
                        "name": "description",
                        "description": "Description of RPC",
                        "schema": {"type": "string"}
                    },
                    {
                        "name": "apis",
                        "description": "Map of all APIs",
                        "schema": {"type": "object"}
                    },
                ],
            },
            "register_plugin": {
                "name": "register_plugin",
                "description": "",
                "args": [
                    {
                        "name": "plugin_info",
                        "description": "",
                        "schema": {"type": "object"}
                    }
                ]
            },
            "register_app": {
                "name": "register_app",
                "description": "",
                "args": [],
            },
            "rpc_info": {
                "name": "rpc_info",
                "description": "",
                "args": [
                    {
                        "name": "package_name",
                        "description": "",
                        "schema": {"type": "string"},
                    },
                    {
                        "name": "rpc_name",
                        "description": "",
                        "schema": {"type": "string"},
                    }
                ]
            }
        },
        "request_queue": "/_system/root_rpc/request",
        "response_queue": "/_system/root_rpc/response"
    }

    return RPCClient(root_rpc_info, token)


class BaseService(object):
    """ Starts the service in the current thread. """
    def __init__(self, token):
        self.rpc_client = get_root_rpc_client(token)
        self.token = token

    def service_start(self):
        self.before_service_start()
        return self.on_service_start()

    def service_stop(self, timeout=None):
        self.on_service_stop()

    def before_service_start(self):
        self.rpc_client.start()
        self.rpc_client["register_app"](_block=True)

    def on_service_start(self, *args, **kwargs):
        pass

    def on_service_stop(self):
        pass

    def wait_for_start(self, timeout):
        pass

    def notify_start(self):
        pass

    @property
    def auth_token(self):
        return self.token


class BackgroundThreadServiceStart(object):
    """ Mixin with BaseServer to start in the background thread. """
    def service_start(self):
        def thread_target():
            with suppress(Exception):
                self.on_service_start()

        self.before_service_start()
        self.service_thread = threading.Thread(target=thread_target)
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

        self.service_proc.stdin.write((self.auth_token + "\n").encode())
        self.service_proc.stdin.flush()

        for line in iter(self.service_proc.stdout.readline, b''):
            content = line.strip().decode()
            if "SERVICE-STARTED-" + name in content:
                self.started_event.set()
            else:
                logger.info("[%s]: %s", name, content)

    def get_launch_command(self, name):
        return ["weave-launch", name]

    def notify_start(self):
        name = '.'.join(self.__module__.split('.')[:-1])
        logger.info("SERVICE-STARTED-" + name)

    def wait_for_start(self, timeout):
        return self.started_event.wait(timeout)


class BasePlugin(BackgroundProcessServiceStart, BaseService):
    def __init__(self, token, config, venv_dir):
        super(BasePlugin, self).__init__(token)
        self.venv_dir = venv_dir
        self.config = config

    def get_launch_command(self, name):
        package_root = self.__module__.split('.')[0]
        py_file = sys.modules[package_root].__file__
        base_dir = os.path.dirname(os.path.dirname(py_file))
        return ["weave-launch", base_dir, self.venv_dir]
