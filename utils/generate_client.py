#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
""" Script/module to generate temporary Taskcluster client via your browser.

This script is mainly useful for Windows users where Taskcluster binaries do not exist.
Nevertheless, you can use it from other platforms.

The authenticate() function returns a generated clientId and accessToken which you
can use for authenticating with Taskcluster service.

Based on original code:
https://github.com/taskcluster/taskcluster-client.py/blob/720b18b7e8b4d5714c31c449a1459d8c5740f8db/taskcluster/utils.py#L322

On Mac OS X the first time it will prompt to grant a Firewall permission to receive incoming
requests.

If you call this module as a script you can pass --print-credentials to output the created
credentials. You can then export them as TASKCLUSTER_ACCESS_TOKEN and TASKCLUSTER_CLIENT_ID
with other taskcluster code to avoid requirying to create a new set of credentials when
you're iterating.

The code supports Python 2 & Python 3 in case we need it for unanticipated reasons.
"""
from __future__ import absolute_import, print_function

import argparse
import os
import random
import string
import sys
import webbrowser

# Mach bootstrap runs Python 2
if sys.version_info[0] < 3:
    from BaseHTTPServer import (BaseHTTPRequestHandler, HTTPServer)
    from urllib import quote
    from urlparse import (parse_qs, urlparse)
else:
    from http.server import (BaseHTTPRequestHandler, HTTPServer)
    from urllib.parse import (quote, parse_qs, urlparse)


creds = [None]


class AuthCallBackRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        query = parse_qs(url.query)
        clientId = query.get('clientId', [None])[0]
        accessToken = query.get('accessToken', [None])[0]
        hasCreds = clientId and accessToken
        if hasCreds:
            creds[0] = {
                "clientId": clientId,
                "accessToken": accessToken
            }
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        if hasCreds:
            self.wfile.write(b"""
                <h1>Credentials transferred successfully</h1>
                <i>You can close this window now.</i>
                <script>window.close();</script>
            """)
        else:
            self.wfile.write(b"""
                <h1>Transfer of credentials failed!</h1>
                <p>Something went wrong, you can navigate back and try again...</p>
            """)
        return


def authenticate(tc_root_url="https://firefox-ci-tc.services.mozilla.com", description=None):
    """
    Open a web-browser to Taskcluster and listen on localhost for
    a callback with credentials in query-string.

    The description will be shown on Taskcluster, if not provided
    a default message will be displayed.
    """
    global creds

    if os.environ.get('TASKCLUSTER_CLIENT_ID') or os.environ.get('TASKCLUSTER_ACCESS_TOKEN'):
        raise Exception(
            'You already have a clientId and access token defined in your'
            'environment variables. Unset the variables or avoid calling this function.'
        )

    if not description:
        description = "Temporary client for use on the command line"

    # Create server on localhost at random port
    retries = 5
    while retries > 0:
        try:
            server = HTTPServer(('', 0), AuthCallBackRequestHandler)
        except Exception:
            retries -= 1
        break
    port = server.server_address[1]

    # Mach bootstrap runs Python 2
    if sys.version_info[0] < 3:
        random_string = ''.join(random.choice(string.ascii_letters) for _ in range(6))
    else:
        random_string = ''.join(random.choices(string.ascii_letters, k=6))

    query = "?scope=%2A&name=cli-{}&expires=1d".format(random_string)
    query += "&callback_url=" + quote('http://localhost:' + str(port), '')
    query += "&description=" + quote(description, '')

    webbrowser.open('{}/auth/clients/create{}'.format(tc_root_url, query), 1, True)
    print("")
    print("-------------------------------------------------------")
    print("  Opening browser window to Taskcluster")
    print("  Asking you to grant temporary credentials to:")
    print("     http://localhost:" + str(port))
    print("-------------------------------------------------------")
    print("")

    server.handle_request()
    return creds[0]


if __name__ == "__main__":
    """
    This script will create a set of credentials on Taskcluster by opening a browser tab.

    If you call it with --print-credentials you will have your client ID and access token
    printed out. You can set these values as environment variables and they will be used
    by Taskcluster libraries.
    """
    parser = argparse.ArgumentParser(description='Script to authenticate with Taskcluster.')
    parser.add_argument(
        '--print-credentials',
        action='store_true',
        help='This will print your Taskcluster credentials once they are created.'
    )
    credentials = authenticate()

    if parser.parse_args().print_credentials:
        print("Set these variables on your environment. The Taskcluster library will use them.")
        print("TASKCLUSTER_CLIENT_ID='{}'".format(credentials["clientId"]))
        print("TASKCLUSTER_ACCESS_TOKEN={}".format(credentials["accessToken"]))
