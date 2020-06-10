import numpy as np

import mo_math
from jx_python import jx
from jx_sqlite.container import Container
from measure_noise import deviance, step_detector
from measure_noise.extract_perf import get_all_signatures, get_signature, get_dataum
from measure_noise.step_detector import find_segments, MAX_POINTS, MIN_POINTS
from measure_noise.utils import assign_colors, histogram
from mo_collections import left
from mo_dots import Null, Data, coalesce, unwrap, listwrap
from mo_future import text, first
from mo_logs import Log, startup, constants
from mo_math.stats import median
from mo_threads import Queue, Thread
from mo_times import MONTH, Date, Timer
from mo_times.dates import parse

IGNORE_TOP = 3  # WHEN CALCULATING NOISE OR DEVIANCE, IGNORE SOME EXTREME VALUES
LOCAL_RETENTION = "3day"  # HOW LONG BEFORE WE REFRESH LOCAL DATABASE ENTRIES
TOLLERANCE = (
    MIN_POINTS
)  # WHEN COMPARING new AND old STEPS, THE NUMBER OF PUSHES TO CONSIDER THEM STILL EQUAL


config = Null
local_container = Null
summary_table = Null
candidates = Null


def process(
    sig_id, show=False, show_limit=MAX_POINTS, show_old=True, show_distribution=None
):
    if not mo_math.is_integer(sig_id):
        Log.error("expecting integer id")
    data = get_dataum(db, sig_id)

    min_date = (Date.today() - 3 * MONTH).unix
    pushes = jx.sort(
        [
            {
                "value": median(rows.value),
                "runs": rows,
                "push": {"time": unwrap(t)["push.time"]},
            }
            for t, rows in jx.groupby(data, "push.time")
            if t["push\\.time"] > min_date
        ],
        "push.time",
    )

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
        dev_status = None
        dev_score = None
        relative_noise = None
    else:
        # MEASURE DEVIANCE (USE THE LAST SEGMENT)
        s, e = new_segments[-2], new_segments[-1]
        last_segment = np.array(values[s:e])
        trimmed_segment = last_segment[np.argsort(last_segment)[IGNORE_TOP:-IGNORE_TOP]]
        dev_status, dev_score = deviance(trimmed_segment)
        relative_noise = np.std(trimmed_segment) / np.mean(trimmed_segment)
        Log.note(
            "\n\tdeviance = {{deviance}}\n\tnoise={{std}}",
            title=title,
            deviance=(dev_status, dev_score),
            std=relative_noise,
        )

    max_extra_diff = None
    max_missing_diff = None

    summary_table.upsert(
        where={"eq": {"id": sig.id}},
        doc=Data(
            id=sig.id,
            title=title,
            num_pushes=len(pushes),
            max_extra_diff=max_extra_diff,
            max_missing_diff=max_missing_diff,
            num_new_segments=len(new_segments),
            relative_noise=relative_noise,
            dev_status=dev_status,
            dev_score=dev_score,
            last_updated=Date.now(),
        ),
    )


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


def show_sorted(sort, limit, where=True, show_distribution=None, show_old=True):
    if not limit:
        return
    tops = summary_table.query(
        {
            "select": "id",
            "where": {
                "and": [{"in": {"id": candidates.id}}, {"gte": {"num_pushes": 1}}]
                + listwrap(where)
            },
            "sort": sort,
            "limit": limit,
            "format": "list",
        }
    ).data

    for id in tops:
        process(id, show=True, show_distribution=show_distribution, show_old=show_old)


def main():
    global local_container, summary_table, candidates
    local_container = Container(kwargs=config.analysis.local_db)
    summary_table = local_container.get_or_create_facts("perf_summary")

    if config.args.id:
        # EXIT EARLY AFTER WE GOT THE SPECIFIC IDS
        if len(config.args.id) < 4:
            step_detector.SHOW_CHARTS = True
        for id in config.args.id:
            process(id, show=True)
        return

    candidates = get_all_signatures(MySQL(config.database), config.analysis.signatures_sql)
    update_local_database()



