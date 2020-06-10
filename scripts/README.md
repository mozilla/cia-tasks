# ETL scripts

In this directory, we have various scripts that can be scheduled via Taskcluster.

## Understanding the model

TBD.

### Login

You must have a Github account to login to the [community cluster](https://community-tc.services.mozilla.com).

### Hooks

TBD

### Executing a task

TODO: Add notes on how authentication is different within a task

## Create a new script

Create a new directory and place your script in there. You can define your own requirements file as well. Create a `README.md` to guide new comers.

For your script to be scheduled you need a hook associated to it. Read the associated section.

If you need to use a new secret read the associated section.

## Create a new secret

In Taskcluster we can store [secrets](https://community-tc.services.mozilla.com/secrets). If you belong to the [CI-A Github team](https://github.com/orgs/mozilla/teams/cia/members) team you can create and fetch secrets. Open [this secret](https://community-tc.services.mozilla.com/secrets/project%2Fcia%2Fgarbage%2Ffoo) to make sure you are part of the team.

Also try to make a test secret to ensure you have the permissions to do so.

### Using secrets locally

There is some documentation about [setup of a taskcluster client](https://github.com/taskcluster/taskcluster/tree/master/clients/client-py#setup).  The specifics are below

To test *locally* that your script can fetch secrets you will have to [download a binary](https://github.com/taskcluster/taskcluster/tree/master/clients/client-shell#readme) to set up your credentials. Unfortunately, this only works for Linux (filed [issue](https://github.com/mozilla/cia-tasks/issues/7)).

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

## Create a new hook

Hooks are used to trigger tasks, and you must [create a hook](https://community-tc.services.mozilla.com/hooks/create). There are a number of options, and you can look over other existing hooks. [Hello World example](https://community-tc.services.mozilla.com/hooks/project-cia/hello-world)

In the task template, be sure to `assume:` the Role ID.

    scopes:
      - 'assume:hook-id:project-cia/etl-schedulers

## Generating credentials for local testing

For Linux and Mac follow the official solution.

For Windows users follow these steps:

    ```shell
    
    ```

## How this framework was set up

We created a [CI-A project in the Taskcluster Community](https://github.com/mozilla/community-tc-config/blob/master/config/projects/cia.yml). This enables us to give permissions to hooks, Github repositories and members
that can add scripts to the project. For the curious, the documentation is defined in [here](https://github.com/mozilla/community-tc-config/blob/master/config/projects/README.md).


## Official documentation

You can find the official documentation here:

* Hooks - [docs](https://community-tc.services.mozilla.com/docs/reference/core/hooks) - [service](https://community-tc.services.mozilla.com/hooks)
* Secrets - [docs](https://community-tc.services.mozilla.com/docs/reference/core/secrets) - [service](https://community-tc.services.mozilla.com/secrets)