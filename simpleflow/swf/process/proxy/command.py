"""
Adapted (much simplified) from https://github.com/inaz2/proxy2.
"""

from __future__ import annotations

import itertools
import select
import socket
import ssl
import sys
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer

import botocore.endpoint

import simpleflow


class ProxyHTTPServer(HTTPServer):
    address_family = socket.AF_INET6

    def handle_error(self, request, client_address):
        # suppress socket/ssl related errors
        cls, e = sys.exc_info()[:2]
        if cls is socket.error or cls is ssl.SSLError:
            pass
        else:
            super().handle_error(request, client_address)


class ProxyRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "Simpleflow Proxy/" + simpleflow.__version__
    error_content_type = "application/json"
    error_message_format = '{"code": %(code)d, "message": "%(message)s", "explain": "%(explain)s"}\n'
    timeout = botocore.endpoint.DEFAULT_TIMEOUT + 1

    def log_error(self, format, *args):
        # suppress "Request timed out: timeout('timed out',)"
        if isinstance(args[0], socket.timeout):
            return

        self.log_message(format, *args)

    # https://en.wikipedia.org/wiki/List_of_Unicode_characters#Control_codes
    _control_char_table = str.maketrans({c: rf"\x{c:02x}" for c in itertools.chain(range(0x20), range(0x7F, 0xA0))})
    _control_char_table[ord("\\")] = r"\\"

    def log_message(self, format, *args):
        """Log an arbitrary message.
        Copy-pasted from gh-100001 in case a vulnerable version of Python is used.

        This is used by all other logging functions.  Override
        it if you have specific logging wishes.

        The first argument, FORMAT, is a format string for the
        message to be logged.  If the format string contains
        any % escapes requiring parameters, they should be
        specified as subsequent arguments (it's just like
        printf!).

        The client ip and current date/time are prefixed to
        every message.

        Unicode control characters are replaced with escaped hex
        before writing the output to stderr.

        """

        message = format % args
        sys.stderr.write(
            f"{self.address_string()} - - [{self.log_date_time_string()}]"
            f" {message.translate(self._control_char_table)}\n"
        )

    def do_CONNECT(self):
        address = self.path.split(":", 1)
        parsed_address = (address[0], int(address[1]) or 443)
        try:
            s = socket.create_connection(parsed_address, timeout=self.timeout)
        except Exception:
            self.send_error(HTTPStatus.BAD_GATEWAY)
            return
        self.send_response(HTTPStatus.OK, "Connection Established")
        self.end_headers()

        self.proxy_connection(s)

    def proxy_connection(self, server_connection: socket.socket) -> None:
        conns = (self.connection, server_connection)
        self.close_connection = False
        while not self.close_connection:
            rlist, wlist, xlist = select.select(conns, [], conns, self.timeout)
            if xlist or not rlist:
                break
            for r in rlist:
                other = conns[1] if r is conns[0] else conns[0]
                data = r.recv(8192)
                if not data:
                    self.close_connection = True
                    break
                other.sendall(data)

    def do_GET(self) -> None:
        if self.path == "/status":
            self.send_response(HTTPStatus.OK)
        else:
            self.send_error(HTTPStatus.NOT_FOUND)


def start_proxy(address: str = "::1", port: int = 4242) -> None:
    server_address = (address, port)
    httpd = ProxyHTTPServer(server_address, ProxyRequestHandler)
    sa = httpd.socket.getsockname()
    print(f"Serving HTTP Proxy on {sa[0]}:{sa[1]}")
    httpd.serve_forever()
