# CIA Tasks

This is a project to support a number of cron jobs and other tasks that support the CIA team

For scheduling tasks read the [scripts](scripts/README.md) README.


## Problem 

There are a number of small tasks that must be run to process and transform data around our various services.  These are too small, too ephemeral, and too indepenedent of other code to put into the main source tree.  At the same time we do not want to be managing the comput resources requires to support these tasks.

## Solution

[The Community TaskCluster!](https://community-tc.services.mozilla.com/)  This will repo will contain the source code for a number of scripts that can run as a task on the community cluster


