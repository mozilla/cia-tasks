#!/usr/bin/env python3
""" Script/module to generate temporary Taskcluster client

This is helpful for Windows and to avoid downloading Taskcluster binaries.

Based on original code:
https://github.com/taskcluster/taskcluster-client.py/blob/720b18b7e8b4d5714c31c449a1459d8c5740f8db/taskcluster/utils.py#L322

On Mac OS X it might prompt to grant a Firewall permission.
"""
import os
import random
import string
import sys
import webbrowser
from http.server import (BaseHTTPRequestHandler, HTTPServer)

from six.moves import urllib
from six.moves.urllib.parse import quote

def authenticate(description=None):
    """
    Open a web-browser to Taskcluster and listen on localhost for
    a callback with credentials in query-string.
    The description will be shown on Taskcluster, if not provided
    a default message with script path will be displayed.
    """
    if not description:
        description = "Temporary client for use on the command line"

    creds = [None]

    class AuthCallBackRequestHandler(BaseHTTPRequestHandler):
        def log_message(format, *args):
            pass

        def do_GET(self):
            url = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(url.query)
            clientId = query.get('clientId', [None])[0]
            accessToken = query.get('accessToken', [None])[0]
            certificate = query.get('certificate', [None])[0]
            hasCreds = clientId and accessToken and certificate
            if hasCreds:
                creds[0] = {
                    "clientId": clientId,
                    "accessToken": accessToken,
                    "certificate": certificate
                }
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            if hasCreds:
                self.wfile.write("""
                    <h1>Credentials transferred successfully</h1>
                    <i>You can close this window now.</i>
                    <script>window.close();</script>
                """)
            else:
                self.wfile.write("""
                    <h1>Transfer of credentials failed!</h1>
                    <p>Something went wrong, you can navigate back and try again...</p>
                """)
            return

    # Create server on localhost at random port
    retries = 5
    while retries > 0:
        try:
            server = HTTPServer(('', 0), AuthCallBackRequestHandler)
        except:
            retries -= 1
        break
    port = server.server_address[1]


    query = "?scope=%2A&name=cli-{}&expires=1d".format(''.join(random.choices(string.ascii_uppercase + string.digits, k=6)))
    query += "&callback_url=" + quote('http://localhost:' + str(port), '')
    query += "&description=" + quote(description, '')

    webbrowser.open('https://community-tc.services.mozilla.com/auth/clients/create' + query, 1, True)
    print("")
    print("-------------------------------------------------------")
    print("  Opening browser window to Taskcluster")
    print("  Asking you to grant temporary credentials to:")
    print("     http://localhost:" + str(port))
    print("-------------------------------------------------------")
    print("")

    while not creds[0]:
        server.handle_request()
    return creds[0]

if __name__ == "__main__":
    authenticate()