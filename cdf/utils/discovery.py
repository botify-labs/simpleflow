from abc import abstractmethod

import cdf.exceptions
from cdf.utils.url import get_domain


class HostDiscovery(object):
    @abstractmethod
    def discover(self, hostname):
        raise NotImplementedError


class UrlHosts(HostDiscovery):
    def discover(self, hostnames):
        def _get_domain(host):
            return get_domain(host) if host.startswith('http') else host

        if isinstance(hostnames, basestring):
            hostnames = [hostnames]

        return [_get_domain(host) for host in hostnames]


class DnsHostDiscovery(HostDiscovery):
    def discover(self, hostname):
        """
        Use DNS to resolve *hostname* to a list of IP addresses.

        :param hostname: to resolve.
        :type  hostname: str.

        :returns:
            :rtype: [str] or [] if *hostname* does not exist.

        """
        import socket

        try:
            result = socket.gethostbyname_ex(hostname)[2]
        except socket.gaierror as err:
            if err.strerror == 'No address associated with hostname':
                raise cdf.exceptions.HostDoesNotExist
            raise

        return result
