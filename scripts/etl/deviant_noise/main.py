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

import numpy as np
import taskcluster

from jx_bigquery import bigquery
from jx_python import jx
from measure_noise import deviance
from measure_noise.analysis import IGNORE_TOP
from measure_noise.step_detector import find_segments
from mo_dots import (
    Data,
    listwrap,
    concat_field,
    set_default,
    coalesce, dict_to_data)
from mo_future import text
from mo_logs import Log
from mo_logs.strings import left
from mo_sql import SQL
from mo_threads import Till, Queue, Thread
from mo_times import Duration, Timer, Date, YEAR, MONTH

LOOK_BACK = 3* MONTH
MAX_RUNTIME = "50minute"  # STOP PROCESSING AFTER THIS GIVEN TIME
SECRET_PREFIX = "project/cia/deviant-noise"
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


def process(
    signature,
    source,
    destination,
):
    min_time = (Date.today() - LOOK_BACK).unix

    # GET SIGNATURE DETAILS
    sig = source.query(SQL(f"""
        SELECT
            signature
        FROM
            treeherder_2d_prod.perf
        WHERE
            job.signature.signature.__s__ = {signature}
        ORDER BY
            push.time.__t__ DESC
        LIMIT 
            1
    """))

    # GET PERF VALUES FOR EACH PUSH
    pushes = source.query(SQL(f"""
        SELECT
            push.time.__t__ as `push.time`, 
            PERCENTILE_CONT (value.__n__, 0.5) AS value
        FROM
            treeherder_2d_prod.perf
        WHERE
            push.time.__t__ > {min_time} AND
            signature.signature.__s__ = {signature}
        GROUP BY 
            push.time.__t__
        ORDER BY
            push.time.__t__
        LIMIT 
            10000
    """))

    values = pushes.value
    title = "-".join(
        map(
            text,
            [
                sig.id,
                sig.framework,
                sig.suite,
                sig.test,
                sig.platform,
                sig.repository.name,
            ],
        )
    )
    Log.note("With {{title}}", title=title)

    with Timer("find segments"):
        new_segments, new_diffs = find_segments(
            values, sig.alert_change_type, sig.alert_threshold
        )

    if len(new_segments) == 1:
        overall_dev_status = None
        overall_dev_score = None
        last_dev_status = None
        last_dev_score = None
        relative_noise = None
    else:
        # NOISE OF LAST SEGMENT
        s, e = new_segments[-2], new_segments[-1]
        last_segment = np.array(values[s:e])
        trimmed_segment = last_segment[np.argsort(last_segment)[IGNORE_TOP:-IGNORE_TOP]]
        last_mean = np.mean(trimmed_segment)
        last_std = np.std(trimmed_segment)
        last_dev_status, last_dev_score = deviance(trimmed_segment)
        relative_noise = last_std / last_mean

        # FOR EACH SEGMENT, NORMALIZE MEAN AND VARIANCE
        normalized = []
        for s, e in jx.pairs(new_segments):
            data = np.array(values[s:e])
            norm = (data + last_mean - np.mean(data)) * last_std / np.std(data)
            normalized.extend(norm)

        trimmed_segment = normalized[np.argsort(normalized)[IGNORE_TOP:-IGNORE_TOP]]
        overall_dev_status, overall_dev_score = deviance(trimmed_segment)
        Log.note(
            "\n\tdeviance = {{deviance}}\n\tnoise={{std}}",
            title=title,
            deviance=(overall_dev_status, overall_dev_score),
            std=relative_noise,
        )

    destination.upsert(
        where={"eq": {"id": sig.id}},
        doc=Data(
            id=sig.id,
            title=title,
            num_pushes=len(pushes),
            num_segments=len(new_segments),
            relative_noise=relative_noise,
            overall_dev_status=overall_dev_status,
            overall_dev_score=overall_dev_score,
            last_mean=last_mean,
            last_std=last_std,
            last_dev_status=last_dev_status,
            last_dev_score=last_dev_score,
            last_updated=Date.now(),
            values=values,
        ),
    )


def main(config):
    outatime = Till(seconds=Duration(MAX_RUNTIME).total_seconds())
    outatime.then(lambda: Log.alert("Out of time, exit early"))

    # GET SOURCE
    source = bigquery.Dataset(config.destination).get_or_create_table(config.source)
    # GET ALL KNOWN SERIES
    min_time = (Date.now() - YEAR).unix
    all_series = dict_to_data({
        id: {"id": id, "last_updated": last_updated}
        for id, last_updated in source.query(SQL(f"""
            SELECT 
                job.signature.signature.__s__ as id, 
                max(push.time.__t__) as last_updated
            FROM 
                treeherder_2d_prod.perf
            WHERE
                push.time.__t__ > {min_time}
            GROUP BY  
                job.signature.signature.__s__
            ORDER BY
                last_updated
            LIMIT 
                5000
        """)).data
    })

    # SETUP DESTINATION
    destination = bigquery.Dataset(config.destination).get_or_create_table(config.destination)

    # PULL PREVIOUS SERIES
    previous = dict_to_data({
        id: {"id": id, "last_processed": last_processed}
        for id, last_processed in destination.query(SQL(f"""
            SELECT
                id,
                MAX(last_updated) as last_processed
            FROM
                dev_2d_devaint.noise
            WHERE
                last_updated.__t__ > {min_time}
            GROUP BY 
                id
            ORDER BY 
                MAX(last_updated)    
            LIMIT 
                5000
        """)).data
    })

    all_series = (all_series | previous).values()

    todo = jx.reverse(jx.sort(all_series, {"last_processed": "desc"})[-5000:])
    needs_update = todo.get("id")
    Log.alert("{{num}} series are candidates for update", num=len(needs_update))

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
                process(sig_id, source, destination)

        threads = [Thread.run(text(i), loop, outatime) for i in range(3)]
        for t in threads:
            t.join()

    Log.note("Local database is up to date")


if __name__ == "__main__":
    with Log.start(app_name="etl") as context:
        main(context.config)


