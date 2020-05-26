#!/usr/bin/env python3
import os

import taskcluster

def main():
    # Use proxy if configured (within a task), otherwise, use local credentials from env vars
    if 'TASKCLUSTER_PROXY_URL' in os.environ:
        options = {'rootUrl': os.environ['TASKCLUSTER_PROXY_URL']}
    else:
        options = taskcluster.optionsFromEnvironment()
    secrets = taskcluster.Secrets(options)
    secret = secrets.get("project/cia/garbage/foo")
    print(secret["secret"])

if __name__ == "__main__":
    main()