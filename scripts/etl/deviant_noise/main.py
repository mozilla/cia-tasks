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
from jx_bigquery.sql import sql_time, quote_column, quote_value
from jx_python import jx
from measure_noise import deviance
from measure_noise.step_detector import find_segments
from mo_dots import (
    Data,
    listwrap,
    concat_field,
    set_default,
    dict_to_data, to_data)
from mo_future import text, first
from mo_json import NUMBER, python_type_to_json_type
from mo_logs import Log
from mo_sql import SQL
from mo_threads import Till, Queue, Thread
from mo_times import Duration, Timer, Date, MONTH

NUM_THREADS = 5
IGNORE_TOP = 2  # IGNORE SOME OUTLIERS
LOOK_BACK = 3 * MONTH
MAX_RUNTIME = "50minute"  # STOP PROCESSING AFTER THIS GIVEN TIME
SECRET_PREFIX = "project/cia/deviant-noise"
SECRET_NAMES = [
    "destination.account_info",
]
# REGISTER float64
python_type_to_json_type[np.float64] = NUMBER


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
    min_time,
    source,
    destination,
):
    # GET SIGNATURE DETAILS
    sig = to_data(first(source.query(SQL(f"""
        SELECT
            signature
        FROM
            treeherder_2d_prod.perf
        WHERE
            signature.signature__hash.__s__ = {quote_value(signature)}
        ORDER BY
            push.time.__t__ DESC
        LIMIT 
            1
    """)))).signature

    # GET PERF VALUES FOR EACH PUSH
    pushes = source.query(SQL(f"""
        SELECT 
            push_time,
            ANY_VALUE(value) as value
        FROM (
            SELECT
                push.time.__t__ as push_time, 
                PERCENTILE_CONT (value.__n__, 0.5) OVER(PARTITION BY push.time.__t__) AS value
            FROM
                treeherder_2d_prod.perf
            WHERE
                push.time.__t__ > {min_time} AND
                signature.signature__hash.__s__ = {quote_value(signature)}
            ) as a 
        GROUP BY 
            push_time
        ORDER BY
            push_time
        LIMIT 
            10000
    """))

    title = "-".join(
        map(
            text,
            [
                sig.framework,
                sig.suite,
                sig.test,
                sig.platform,
                sig.repository,
            ],
        )
    )

    values = [p['value'] for p in pushes]
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
        trimmed_segment = last_segment
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

        trimmed_segment = normalized
        overall_dev_status, overall_dev_score = deviance(trimmed_segment)
        Log.note(
            "\n\tdeviance = {{deviance}}\n\tnoise={{std}}\n\tpushes={{pushes}}",
            title=title,
            deviance=(overall_dev_status, overall_dev_score),
            std=relative_noise,
            pushes=len(values)
        )

    destination.add(
        Data(
            id=signature,
            title=title,
            num_pushes=len(values),
            num_segments=len(new_segments)-1,
            relative_noise=relative_noise,
            overall_dev_status=overall_dev_status,
            overall_dev_score=overall_dev_score,
            last_mean=last_mean,
            last_std=last_std,
            last_dev_status=last_dev_status,
            last_dev_score=last_dev_score,
            last_updated=Date.now(),
            values=values,
        )
    )


def main(config):
    outatime = Till(seconds=Duration(MAX_RUNTIME).total_seconds())
    outatime.then(lambda: Log.alert("Out of time, exit early"))

    # GET SOURCE
    source = bigquery.Dataset(config.source).get_or_create_table(read_only=True, kwargs=config.source)

    # SETUP DESTINATION
    destination = bigquery.Dataset(config.destination).get_or_create_table(config.destination)
    # ENSURE SHARDS ARE MERGED
    destination.merge_shards()

    # GET ALL KNOWN SERIES
    min_time = sql_time((Date.today() - LOOK_BACK))
    all_series = dict_to_data({
        doc['id']: doc
        for doc in source.query(SQL(f"""
            SELECT 
                s.signature.signature__hash.__s__ as id, 
                max(s.push.time.__t__) as last_updated
            FROM 
                {quote_column(source.full_name)} as s
            LEFT JOIN
                {quote_column(destination.full_name)} AS d ON d.id = signature.signature__hash.__s__  
            WHERE
                d.id is NULL AND
                s.push.time.__t__ > {min_time} AND
                s.signature.signature__hash.__s__ IS NOT NULL AND
                s.push.repository.__s__ IN ("autoland", "mozilla-central")
            GROUP BY  
                s.signature.signature__hash.__s__
            ORDER BY
                last_updated DESC  # MOST ACTIVE FIRST
            LIMIT 
                5000
        """))
    })

    # PULL PREVIOUS SERIES
    previous = dict_to_data({
        doc['id']: doc
        for doc in destination.query(SQL(f"""
            SELECT
                id,
                MAX(last_updated) as last_processed
            FROM
                {quote_column(destination.full_name)}
            WHERE
                last_updated > {min_time} AND
                num__pushes.__i__ > 0
            GROUP BY 
                id
            ORDER BY 
                MAX(last_updated)    
            LIMIT 
                5000
        """))
    })

    all_series = (all_series | previous).values()

    todo = jx.reverse(jx.sort(all_series, {"last_processed": "desc"})).limit(5000)
    needs_update = todo.get("id")
    Log.alert("{{num}} series are candidates for update", num=len(needs_update))

    limited_update = Queue("sigs")
    limited_update.extend(needs_update)

    with Timer("Updating local database"):
        def loop(please_stop):
            while not please_stop:
                sig_id = limited_update.pop_one()
                if not sig_id:
                    return
                try:
                    process(sig_id, min_time, source, destination)
                except Exception as cause:
                    Log.warning("Could not process {{sig}}", sig=sig_id, cause=cause)
        threads = [Thread.run(text(i), loop, please_stop=outatime) for i in range(NUM_THREADS)]
        for t in threads:
            t.join()

    destination.merge_shards()

    Log.note("Local database is up to date")


if __name__ == "__main__":
    with Log.start(app_name="etl") as config:
        main(config)


