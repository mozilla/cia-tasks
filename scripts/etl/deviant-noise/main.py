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



from jx_bigquery import bigquery
from jx_bigquery.sql import quote_column, quote_value
from jx_mysql.mysql import MySQL
from jx_python import jx
from measure_noise.analysis_etl import process
from mo_dots import dict_to_data, listwrap, Data, concat_field, set_default
from mo_future import text
from mo_logs import Log
from mo_sql import SQL
from mo_threads import Till, Queue, Thread
from mo_times import Duration, Timer, Date, MONTH, DAY

NUM_THREADS = 5
LOOK_BACK = 3 * MONTH
MAX_RUNTIME = "50minute"  # STOP PROCESSING AFTER THIS GIVEN TIME
STALE = 3 * DAY  # DO NOT UPDATE DATA THAT IS NOT STALE
SECRET_PREFIX = "project/cia/deviant_noise"
SECRET_NAMES = [
    "destination.account_info",
    "source"
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
    if config.inject_secrets != False:
        with Timer("get secrets"):
            import taskcluster
            secrets = taskcluster.Secrets(config.taskcluster)
            acc = Data()
            for s in listwrap(SECRET_NAMES):
                secret_name = concat_field(SECRET_PREFIX, s)
                Log.note("get secret named {{name|quote}}", name=secret_name)
                acc[s] = secrets.get(secret_name)["secret"]
            config |= acc


def main(config):
    outatime = Till(seconds=Duration(MAX_RUNTIME).total_seconds())
    outatime.then(lambda: Log.alert("Out of time, exit early"))
    since = Date.today()-LOOK_BACK

    # SETUP DESTINATION
    destination = bigquery.Dataset(config.destination).get_or_create_table(config.destination)
    # ENSURE SHARDS ARE MERGED
    destination.merge_shards()

    # GET ALL KNOWN SERIES
    with MySQL(config.source) as t:
        recently_updated_series = dict_to_data({
            doc['id']: doc
            for doc in t.query(SQL(f"""
                SELECT MAX(s.signature_hash) id
                FROM (            
                    SELECT d.signature_id, d.push_timestamp
                    FROM performance_datum d 
                    WHERE d.repository_id IN (77, 1)  -- autoland, mozilla-central
                    ORDER BY d.id desc
                    LIMIT 1000000
                ) d
                LEFT JOIN performance_signature s on s.id= d.signature_id
                WHERE s.test IS NULL or s.test='' or s.test=s.suite
                GROUP BY d.signature_id
                ORDER BY MAX(d.push_timestamp) DESC
            """))
        })

    # PULL PREVIOUS SERIES
    recently_scanned = dict_to_data({
        doc['id']: doc
        for doc in destination.sql_query(SQL(f"""
            SELECT
                id,
                MAX(last_updated) as last_processed
            FROM
                {quote_column(destination.full_name)}
            GROUP BY 
                id
            HAVING 
                MAX(last_updated) >= {quote_value(Date.now()-STALE)}
        """))
    })

    todo = [v for k, v in recently_updated_series.items() if k not in recently_scanned]
    todo = jx.reverse(jx.sort(todo, {"last_processed": "desc"})).limit(5000)
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
                    process(sig_id, since, config.source, destination)
                except Exception as cause:
                    Log.warning("Could not process {{sig}}", sig=sig_id, cause=cause)
        threads = [Thread.run(text(i), loop, please_stop=outatime) for i in range(NUM_THREADS)]
        for t in threads:
            t.join()

    destination.merge_shards()

    Log.note("Local database is up to date")


if __name__ == "__main__":
    with Log.start(app_name="etl") as config:
        inject_secrets(config)
        main(config)


