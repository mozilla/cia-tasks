# Hello world script

This script fetches a secret and displays it.

```shell
cd scripts/hello_world
python -m venv .venv
source .venv/bin/activate # Windows: Scripts/install/activate
pip install -r requirements.txt
# Download authentication binary https://github.com/taskcluster/taskcluster/tree/master/clients/client-shell#readme
# This will open a Taskcluster client creation page and return back to the CLI
# It will set the variables TASKCLUSTER_CLIENT_ID and TASKCLUSTER_TOKEN
# TODO: Add instructions for Windows as there's no binary
export TASKCLUSTER_ROOT_URL=https://community-tc.services.mozilla.com
eval `taskcluster signin`
# The script will fetch a secret from Taskcluster and output the contents of it
./hello_world.py
```
