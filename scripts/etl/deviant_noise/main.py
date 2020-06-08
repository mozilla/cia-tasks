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

from logs import capture_logging, capture_loguru
from mo_dots import (
    Data,
    listwrap,
    concat_field,
    set_default,
)
from mo_logs import startup, constants, Log
from mo_threads import Till
from mo_times import Duration, Timer, MINUTE

MAX_RUNTIME = "50minute"  # STOP PROCESSING AFTER THIS GIVEN TIME
DEFAULT_START = "today-2day"
LOOK_BACK = 30
LOOK_FORWARD = 30
CACHY_STATE = "cia-tasks/etl/schedules"
CACHY_RETENTION = Duration("30day") / MINUTE
SHOW_S3_CACHE_HIT = True
SECRET_PREFIX = "project/cia/smart-scheduling"
SECRET_NAMES = [
    "destination.account_info",
]


def inject_secrets(config):
    """
    INJECT THE SECRETS INTO THE CONFIGURATION
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


def main():
    with Log.start(app_name="etl") as context:
        config = context.config
        outatime = Till(seconds=Duration(MAX_RUNTIME).total_seconds())
        outatime.then(lambda: Log.alert("Out of time, exit early"))

        # PULL SOME SERIES

        def update_local_database():
    # GET EVERYTHING WE HAVE SO FAR
    exists = summary_table.query(
        {
            "select": ["id", "last_updated"],
            "where": {"and": [{"in": {"id": candidates.id}}, {"exists": "num_pushes"}]},
            "sort": "last_updated",
            "limit": 100000,
            "format": "list",
        }
    ).data
    # CHOOSE MISSING, THEN OLDEST, UP TO "RECENT"
    missing = list(set(candidates.id) - set(exists.id))

    too_old = Date.today() - parse(LOCAL_RETENTION)
    needs_update = missing + [e.id for e in exists if e.last_updated < too_old.unix]
    Log.alert("{{num}} series are candidates for local update", num=len(needs_update))

    limited_update = Queue("sigs")
    limited_update.extend(
        left(needs_update, coalesce(config.analysis.download_limit, 100))
    )
    Log.alert("Updating local database with {{num}} series", num=len(limited_update))

    with Timer("Updating local database"):

        def loop(please_stop):
            while not please_stop:
                sig_id = limited_update.pop_one()
                if not sig_id:
                    return
                process(sig_id)

        threads = [Thread.run(text(i), loop) for i in range(3)]
        for t in threads:
            t.join()

    Log.note("Local database is up to date")






    # PERFORM CALC
        # PUSH RESULTS

if __name__ == "__main__":
    main()


