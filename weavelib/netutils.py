import logging
import re
import socket
from subprocess import Popen, PIPE, DEVNULL
from sys import platform

import netifaces
from ipaddress import IPv4Network


logger = logging.getLogger(__name__)


def get_mac_address(host):
    """ Returns MAC address for a hostname. """
    mac_pattern = '(([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2})'
    try:
        host = socket.gethostbyname(host)
    except socket.error:
        pass

    if "linux" in platform:
        command = ["arp", "-a", host]
    elif "darwin" in platform:
        command = ["arp", host]
    else:
        raise Exception("Unsupported platform.")

    with Popen(command, stdout=PIPE) as proc:
        for line in proc.stdout:
            line = line.decode("UTF-8")
            if host in line:
                matches = re.findall(mac_pattern, line)
                if matches:
                    return matches[0][0]
        return None


def iter_ipv4_addresses():
    for iface in netifaces.interfaces():
        for ip_obj in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
            if "netmask" in ip_obj and "addr" in ip_obj:
                yield ip_obj


def relevant_ipv4_address(ip_addr):
    """
    Returns machine's IPv4 address belonging to the same interface as ip_addr.
    """
    for ip_obj in iter_ipv4_addresses():
        ours = IPv4Network(ip_obj["addr"] + "/" + ip_obj["netmask"],
                           strict=False)
        theirs = IPv4Network(ip_addr + "/" + ip_obj["netmask"], strict=False)
        if ours == theirs:
            return ip_obj["addr"]

    return None


def ping_host(host):
    with Popen(["ping", "-c1", "-w2", host], stdout=DEVNULL) as proc:
        proc.wait()
        return proc.returncode == 0
