# ETL scripts

In this directory, we will have various scripts that we can schedule via Taskcluster.

## Requirements

Python and pip

## How to get started

Create a new directory and place your desired script in there.
You can define your own requirements file as well.

## How to schedule a task

For now, we will have to create a hook per script and we will have to create it by hand.
You can see [this hook](https://community-tc.services.mozilla.com/hooks/project-cia/hello-world) as
a simple example. It checks out this repo, set ups the virtualenv and executes the script.

Copy the contents of that hook and create a new hook. Only modify the `command` entry to meet your needs.
Adjust the `cron` schedule in the UI.

Note that you can manually trigger a hook (without waiting for its schedule) and that will schedule a task
executing the contents of the hook.

In the future, if we decide that we want to improve this system we can define our hooks in this repo and
get them deployed automatically rather manually defining them (filed [issue](https://github.com/mozilla/cia-tasks/issues/5)).

## Using secrets

In Taskcluster we can store [secrets](https://community-tc.services.mozilla.com/secrets).
If you belong to the [CI-A Github team](https://github.com/orgs/mozilla/teams/cia/members) team you can create and fetch secrets.
Open [this secret](https://community-tc.services.mozilla.com/secrets/project%2Fcia%2Fgarbage%2Ffoo) and try to see it. If you can see the
secret you can then create your own.

### Retrieving secrets locally

#### Via official binaries (Mac & Linux)

To test *locally* that your script can fetch secrets you will have to [download a binary](https://github.com/taskcluster/taskcluster/tree/master/clients/client-shell#readme)
to set up your credentials. Unfortunately, this only works for Linux and Windows (filed [issue](https://github.com/mozilla/cia-tasks/issues/7)).

On Mac OS X, you will need to right click the binary and Open it. That will except the binary from some security measures.

Once you run the following you will be signed in through the browser and it will set some TASKCLUSTER_* env variables
in your shell which are needed to authenticate:

```shell
export TASKCLUSTER_ROOT_URL=https://community-tc.services.mozilla.com
eval `~/Desktop/taskcluster signin`
```

This is a code snippet that shows you how a secret is fetched using env variables defined by the previous step:

```python
import taskcluster
# This will read the environment variables and authenticate you
secrets = taskcluster.Secrets(taskcluster.optionsFromEnvironment())
secret = secrets.get("project/cia/garbage/foo")
print(secret["secret"])
```

#### Via script

This WIP.

On Mac OS X it might prompt to grant a Firewall permission.

```shell
cd utils
# This will open a browser tab, sign in and save the generated client
# Upon saving you can close the tab and return to the command line
poetry run generate_client.py
# TBD
```


## How this is set up

We set up a CI-A project in the Taskcluster Community set up (see [configuration](https://github.com/mozilla/community-tc-config/blob/master/config/projects/cia.yml)).
It's documentation is defined in [here](https://github.com/mozilla/community-tc-config/blob/master/config/projects/README.md).

## Official documentation

You can find the official documentation here:

* Hooks - [docs](https://community-tc.services.mozilla.com/docs/reference/core/hooks) - [service](https://community-tc.services.mozilla.com/hooks)
* Secrets - [docs](https://community-tc.services.mozilla.com/docs/reference/core/secrets) - [service](https://community-tc.services.mozilla.com/secrets)