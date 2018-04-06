import netifaces
import pytest

import weavelib.netutils as netutils


class TestRelevantIPv4Address(object):
    def setup_method(self):
        self.original_interfaces = netutils.netifaces.interfaces
        self.original_ifaddresses = netutils.netifaces.ifaddresses

    def teardown_method(self):
        netutils.netifaces.interfaces = self.original_interfaces
        netutils.netifaces.ifaddresses = self.original_ifaddresses

    def test_call(self):
        expected = [
            {"netmask": "255.255.255.0", "addr": "192.168.1.4"},
            {"netmask": "255.255.255.0", "addr": "162.168.5.4"},
        ]
        res = {
            "if1": {netifaces.AF_INET: [expected[0]]},
            "if2": {},
            "if3": {netifaces.AF_INET: [expected[1]]},
        }

        netutils.netifaces.interfaces = lambda: ["if1", "if2", "if3"]
        netutils.netifaces.ifaddresses = lambda x: res[x]

        assert netutils.relevant_ipv4_address("192.168.1.114") == "192.168.1.4"
        assert netutils.relevant_ipv4_address("162.168.5.94") == "162.168.5.4"
