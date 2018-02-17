"""
Contains a base class for all services. All services must inherit
BaseService. If they wish to start in background, they should use the
BackgroundServiceStart mixin before BaseService while inheriting.
"""


import logging
import os
import subprocess
import threading
from contextlib import suppress

import psutil
from jsonschema import Draft4Validator

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
                        "name": "request_schema",
                        "description": "Request JSONSchema of the RPC",
                        "schema": Draft4Validator.META_SCHEMA
                    },
                    {
                        "name": "response_schema",
                        "description": "Response JSONSchema of the RPC",
                        "schema": Draft4Validator.META_SCHEMA
                    },
                ],
            },
            "register_app_view": {
                "name": "register_app_view",
                "description": "",
                "args": [
                    {
                        "name": "object",
                        "description": "View to register",
                        "schema": {"type": "object"}
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

    def on_service_start(self, *args, **kwargs):
        pass

    def on_service_stop(self):
        pass

    def wait_for_start(self, timeout):
        pass

    def notify_start(self):
        pass

    def get_component_name(self):
        raise NotImplementedError("Must override.")

    def get_service_queue_name(self, queue_name):
        # TODO: Here is not a good choice. Move elsewhere.
        service_name = self.get_component_name()
        return "/services/{}/{}".format(service_name, queue_name)
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
        name = self.get_component_name()
        logger.info("Stopping background process: %s", name)
        psutil.Process(self.service_pid).terminate()
        try:
            self.service_proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            psutil.Process(self.service_pid).kill()

    def child_process(self):
        name = self.get_component_name()
        command = ["weave-launch", name]
        self.service_proc = subprocess.Popen(command, env=os.environ.copy(),
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.STDOUT)
        self.service_pid = self.service_proc.pid
        for line in iter(self.service_proc.stdout.readline, b''):
            content = line.strip().decode()
            if "SERVICE-STARTED-" + name in content:
                self.started_event.set()
            else:
                logger.info("[%s]: %s", name, content)

    def notify_start(self):
        logger.info("SERVICE-STARTED-" + self.get_component_name())

    def wait_for_start(self, timeout):
        return self.started_event.wait(timeout)
