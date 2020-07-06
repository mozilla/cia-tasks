# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

import numpy as np

import mo_math
from jx_python import jx
from measure_noise import deviance, step_detector
from measure_noise.extract_perf import get_all_signatures, get_signature, get_dataum
from measure_noise.step_detector import find_segments, MAX_POINTS, MIN_POINTS
from measure_noise.utils import assign_colors, histogram
from mo_collections import left
from mo_dots import Null, Data, coalesce, unwrap, listwrap
from mo_future import text
from mo_logs import Log, startup, constants
from mo_math.stats import median
from mo_threads import Queue, Thread
from mo_times import MONTH, Date, Timer
from mo_times.dates import parse

IGNORE_TOP = 3  # WHEN CALCULATING NOISE OR DEVIANCE, IGNORE SOME EXTREME VALUES
LOCAL_RETENTION = "3day"  # HOW LONG BEFORE WE REFRESH LOCAL DATABASE ENTRIES
# WHEN COMPARING new AND old STEPS, THE NUMBER OF PUSHES TO CONSIDER THEM STILL EQUAL
TOLERANCE = MIN_POINTS
LOOK_BACK = 3 * MONTH


config = Null
local_container = Null
summary_table = Null
candidates = Null


def process(
    signature_hash,
    since,
    source,
    show=False,
    show_limit=MAX_POINTS,
    show_old=True,
    show_distribution=None
):
    """
    :param signature_hash: The performance hash
    :param since: Only data after this date
    :param show:
    :param show_limit:
    :param show_old:
    :param show_distribution:
    :return:
    """
    if not mo_math.is_hex(signature_hash):
        Log.error("expecting hexidecimal hash")

    # GET SIGNATURE DETAILS
    sig = get_signature(source, signature_hash)

    # GET SIGNATURE DETAILS
    data = get_dataum(source, signature_hash, since)

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

    values = list(pushes.value)
    title = "-".join(
        map(
            str,
            [
                sig.framework,
                sig.suite,
                sig.test,
                sig.platform,
                sig.repository,
            ],
        )
    )
    Log.note("With {{title}}", title=title)

    with Timer("find segments"):
        new_segments, new_diffs = find_segments(
            values, sig.alert_change_type, sig.alert_threshold
        )

    # USE PERFHERDER ALERTS TO IDENTIFY OLD SEGMENTS
    old_segments = tuple(
        sorted(
            set(
                [i for i, p in enumerate(pushes) if any(r.alert.id for r in p.runs)]
                + [0, len(pushes)]
            )
        )
    )
    old_medians = [0.0] + [
        np.median(values[s:e]) for s, e in zip(old_segments[:-1], old_segments[1:])
    ]
    old_diffs = np.array(
        [b / a - 1 for a, b in zip(old_medians[:-1], old_medians[1:])] + [0]
    )

    if len(new_segments) == 1:
        overall_dev_status = None
        overall_dev_score = None
        last_mean = None
        last_std = None
        last_dev_status = None
        last_dev_score = None
        relative_noise = None
    else:
        # NOISE OF LAST SEGMENT
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

        if show_distribution:
            histogram(last_segment, title=dev_status+"="+text(dev_score))

    max_extra_diff = None
    max_missing_diff = None
    _is_diff = is_diff(new_segments, old_segments)
    if _is_diff:
        # FOR MISSING POINTS, CALC BIGGEST DIFF
        max_extra_diff = mo_math.MAX(
            abs(d)
            for s, d in zip(new_segments, new_diffs)
            if all(not (s - TOLERANCE <= o <= s + TOLERANCE) for o in old_segments)
        )
        max_missing_diff = mo_math.MAX(
            abs(d)
            for s, d in zip(old_segments, old_diffs)
            if all(not (s - TOLERANCE <= n <= s + TOLERANCE) for n in new_segments)
        )

        Log.alert(
            "Disagree max_extra_diff={{max_extra_diff|round(places=3)}}, max_missing_diff={{max_missing_diff|round(places=3)}}",
            max_extra_diff=max_extra_diff,
            max_missing_diff=max_missing_diff,
        )
        Log.note("old={{old}}, new={{new}}", old=old_segments, new=new_segments)
        if show and len(pushes):
            show_old and assign_colors(values, old_segments, title="OLD " + title)
            assign_colors(values, new_segments, title="NEW " + title)
    else:
        Log.note("Agree")
        if show and len(pushes):
            show_old and assign_colors(values, old_segments, title="OLD " + title)
            assign_colors(values, new_segments, title="NEW " + title)

    summary_table.upsert(
        where={"eq": {"id": sig.id}},
        doc=Data(
            id=sig.id,
            title=title,
            num_pushes=len(pushes),
            is_diff=_is_diff,
            max_extra_diff=max_extra_diff,
            max_missing_diff=max_missing_diff,
            num_new_segments=len(new_segments),
            num_old_segments=len(old_segments),
            relative_noise=relative_noise,
            dev_status=dev_status,
            dev_score=dev_score,
            last_updated=Date.now(),
        ),
    )


def is_diff(A, B):
    if len(A) != len(B):
        return True

    for a, b in zip(A, B):
        if b - TOLERANCE <= a <= b + TOLERANCE:
            continue
        else:
            return True
    return False


def update_local_database(since):
    # GET EVERYTHING WE HAVE SO FAR
    exists = summary_table.query(
        {
            "select": ["signature_hash", "last_updated"],
            "where": {"and": [{"in": {"id": candidates.id}}, {"exists": "num_pushes"}]},
            "sort": "last_updated",
            "limit": 100000,
            "format": "list",
        }
    ).data
    # CHOOSE MISSING, THEN OLDEST, UP TO "RECENT"
    missing = list(set(candidates.signature_hash) - set(exists.signature_hash))

    too_old = Date.today() - parse(LOCAL_RETENTION)
    needs_update = missing + [e.signature_hash for e in exists if e.last_updated < too_old.unix]
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
                process(sig_id, since)

        threads = [Thread.run(text(i), loop) for i in range(3)]
        for t in threads:
            t.join()

    Log.note("Local database is up to date")


def show_sorted(sort, limit, where=True, show_distribution=None, show_old=True):
    if not limit:
        return
    tops = summary_table.query(
        {
            "select": "signature_hash",
            "where": {
                "and": [{"in": {"signature_hash": candidates.signature_hash}}, {"gte": {"num_pushes": 1}}]
                + listwrap(where)
            },
            "sort": sort,
            "limit": limit,
            "format": "list",
        }
    ).data

    for signature_hash in tops:
        process(signature_hash, show=True, show_distribution=show_distribution, show_old=show_old)


def main():
    global local_container, summary_table, candidates

    from jx_sqlite.container import Container

    since = Date.today()-LOOK_BACK
    local_container = Container(kwargs=config.analysis.local_db)
    summary_table = local_container.get_or_create_facts("deviant_summary")

    if config.args.id:
        # EXIT EARLY AFTER WE GOT THE SPECIFIC IDS
        if len(config.args.id) < 4:
            step_detector.SHOW_CHARTS = True
        for id in config.args.id:
            process(id, since=since, show=True)
        return

    candidates = get_all_signatures(config.database, config.analysis.signatures_sql)
    if not config.args.now:
        update_local_database(since)

    # DEVIANT
    show_sorted(
        sort={"value": {"abs": "dev_score"}, "sort": "desc"},
        limit=config.args.deviant,
        show_old=False,
        show_distribution=True,
    )

    # MODAL
    show_sorted(
        sort="dev_score",
        limit=config.args.modal,
        where={"eq": {"dev_status": "MODAL"}},
        show_distribution=True,
    )

    # OUTLIERS
    show_sorted(
        sort={"value": "dev_score", "sort": "desc"},
        limit=config.args.outliers,
        where={"eq": {"dev_status": "OUTLIERS"}},
        show_distribution=True,
    )

    # SKEWED
    show_sorted(
        sort={"value": {"abs": "dev_score"}, "sort": "desc"},
        limit=config.args.skewed,
        where={"eq": {"dev_status": "SKEWED"}},
        show_distribution=True,
    )

    # OK
    show_sorted(
        sort={"value": {"abs": "dev_score"}, "sort": "desc"},
        limit=config.args.ok,
        where={"eq": {"dev_status": "OK"}},
        show_distribution=True,
    )

    # NOISE
    show_sorted(
        sort={"value": {"abs": "relative_noise"}, "sort": "desc"},
        limit=config.args.noise,
    )

    # EXTRA
    show_sorted(
        sort={"value": {"abs": "max_extra_diff"}, "sort": "desc"},
        where={"lte": {"num_new_segments": 7}},
        limit=config.args.extra,
    )

    # MISSING
    show_sorted(
        sort={"value": {"abs": "max_missing_diff"}, "sort": "desc"},
        where={"lte": {"num_old_segments": 6}},
        limit=config.args.missing,
    )

    # PATHOLOGICAL
    show_sorted(
        sort={"value": "num_new_segments", "sort": "desc"},
        limit=config.args.pathological,
    )


if __name__ == "__main__":
    config = startup.read_settings(
        [
            {
                "name": ["--id", "--key", "--ids", "--keys"],
                "dest": "id",
                "nargs": "*",
                "type": int,
                "help": "show specific signatures",
            },
            {
                "name": "--now",
                "dest": "now",
                "help": "do not update signatures, go direct to showing problems with what is known locally",
                "action": "store_true",
            },
            {
                "name": ["--dev", "--deviant", "--deviance"],
                "dest": "deviant",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of top deviant series",
                "action": "store",
            },
            {
                "name": ["--modal"],
                "dest": "modal",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of top modal series",
                "action": "store",
            },
            {
                "name": ["--outliers"],
                "dest": "outliers",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of top outliers series",
                "action": "store",
            },
            {
                "name": ["--skewed", "--skew"],
                "dest": "skewed",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of top skewed series",
                "action": "store",
            },
            {
                "name": ["--ok"],
                "dest": "ok",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of top worst OK series",
                "action": "store",
            },
            {
                "name": ["--noise", "--noisy"],
                "dest": "noise",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of top noisiest series",
                "action": "store",
            },
            {
                "name": ["--extra", "-e"],
                "dest": "extra",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of series that are missing perfherder alerts",
                "action": "store",
            },
            {
                "name": ["--missing", "--miss", "-m"],
                "dest": "missing",
                "nargs": "?",
                "const": 10,
                "type": int,
                "help": "show number of series which are missing alerts over perfherder",
                "action": "store",
            },
            {
                "name": ["--pathological", "--pathological", "--pathology", "-p"],
                "dest": "pathological",
                "nargs": "?",
                "const": 3,
                "type": int,
                "help": "show number of series that have most edges",
                "action": "store",
            },
        ]
    )
    constants.set(config.constants)
    try:
        Log.start(config.debug)
        main()
    except Exception as e:
        Log.warning("Problem with perf scan", e)
    finally:
        Log.stop()
