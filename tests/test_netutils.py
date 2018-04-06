import netifaces
import pytest

import weavelib.netutils as netutils


@pytest.fixture(params=[("linux", True), ("darwin", True), ("other", False)])
def platform(request):
    return request.param


class MockPopen(object):
    def __init__(self, text):
        self.stdout = [x.encode() for x in text.splitlines()]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestGetMacAddress(object):
    def setup_method(self):
        self.original_platform = netutils.platform
        self.original_popen = netutils.Popen

    def teardown_method(self):
        netutils.Popen = self.original_popen

    def test_get_mac_adddress(self, platform):
        platform_name, status = platform
        self.setup_platform(platform_name)
        self.setup_mock(platform_name)

        if not status:
            with pytest.raises(Exception):
                netutils.get_mac_address("blah")
            return

        assert netutils.get_mac_address("1.1.1.1") == "12:34:56:AB:78:90"
        assert netutils.get_mac_address("1.2.3.4") is None

    def setup_platform(self, platform):
        netutils.platform = platform

    def setup_mock(self, platform):
        good_output = """random text
           1.1.1.1 here's mac: 12:34:56:AB:78:90
         more text
        """

        def patched_popen(command, stdout):
            if command[:2] == ["arp", "-a"]:
                return MockPopen(good_output)
            elif len(command) == 2 and command[0] == "arp":
                return MockPopen(good_output)
            else:
                return MockPopen("random text")

        netutils.Popen = patched_popen


class TestIterIPv4Addresses(object):
    def setup_method(self):
        self.original_interfaces = netutils.netifaces.interfaces
        self.original_ifaddresses = netutils.netifaces.ifaddresses

    def teardown_method(self):
        netutils.netifaces.interfaces = self.original_interfaces
        netutils.netifaces.ifaddresses = self.original_ifaddresses

    def test_call(self):
        expected = [
            {"netmask": "test1", "addr": "test2"},
            {"netmask": "test3", "addr": "test4"},
        ]
        res = {
            "if1": {netifaces.AF_INET: [expected[0]]},
            "if2": {},
            "if3": {netifaces.AF_INET: [expected[1]]},
        }

        netutils.netifaces.interfaces = lambda: ["if1", "if2", "if3"]
        netutils.netifaces.ifaddresses = lambda x: res[x]

        assert list(netutils.iter_ipv4_addresses()) == expected


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
        assert netutils.relevant_ipv4_address("172.168.5.94") is None
