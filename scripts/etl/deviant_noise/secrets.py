# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import taskcluster

from mo_dots import Data, listwrap, concat_field, set_default
from mo_logs import Log
from mo_times import Timer

SECRET_PREFIX = "project/cia/deviant-noise"
SECRET_NAMES = [
    "destination.account_info",
    "source.account_info",
]


def inject_secrets(config):
    """
    INJECT THE SECRETS INTO config
    :param config: CONFIG DATA

    ************************************************************************
    ** ENSURE YOU HAVE AN ENVIRONMENT VARIABLE SET:
    ** TASKCLUSTER_ROOT_URL = https://community-tc.services.mozilla.com
    ************************************************************************
    """
    with Timer("get secrets"):
        secrets = taskcluster.Secrets(config.taskcluster)
        acc = Data()
        for s in listwrap(SECRET_NAMES):
            secret_name = concat_field(SECRET_PREFIX, s)
            Log.note("get secret named {{name|quote}}", name=secret_name)
            acc[s] = secrets.get(secret_name)["secret"]
        set_default(config, acc)


