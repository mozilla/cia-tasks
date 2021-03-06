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

import os

import adr
import taskcluster
from adr import configuration as adr_configuration
from adr.configuration import Configuration, CustomCacheManager
from adr.errors import MissingDataError
from adr.util.cache_stores import S3Store
from mozci.push import make_push_objects

import mo_math
from jx_bigquery import bigquery
from jx_python import jx
from logs import capture_logging, capture_loguru
from mo_dots import (
    Data,
    coalesce,
    listwrap,
    wrap,
    concat_field,
    set_default,
)
from mo_json import value2json, json2value
from mo_logs import startup, constants, Log
from mo_threads import Process, Till
from mo_threads.repeat import Repeat
from mo_times import Date, Duration, Timer, MINUTE
from pyLibrary.env import git
from pyLibrary.meta import extend

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


class Schedulers:
    def __init__(self, config):
        self.config = config = wrap(config)
        config.range.min = Date(config.range.min)
        config.range.max = Date(config.range.max)
        config.start = Date(config.start)
        config.interval = Duration(config.interval)
        config.branches = listwrap(config.branches)
        self.destination = bigquery.Dataset(config.destination).get_or_create_table(
            config.destination
        )

        # CALCULATE THE PREVIOUS RUN
        mozci_version = self.version("mozci")
        prev_done = self.get_state()
        if prev_done and prev_done.mozci_version == mozci_version:
            self.done = Data(
                mozci_version=mozci_version,
                min=Date(coalesce(prev_done.min, config.start, "today-2day")),
                max=Date(coalesce(prev_done.max, config.start, "today-2day")),
            )
        else:
            self.done = Data(
                mozci_version=mozci_version,
                min=Date(coalesce(config.start, "today-2day")),
                max=Date(coalesce(config.start, "today-2day")),
            )
            self.set_state()

    def get_state(self):
        try:
            state = json2value(adr_configuration.config.cache.get(CACHY_STATE))
            Log.note("Got ETL state:\n{{state|json|indent}}", state=state)
            return state
        except Exception:
            return None

    def set_state(self):
        adr_configuration.config.cache.put(CACHY_STATE, value2json(self.done), minutes=CACHY_RETENTION)

    def version(self, package):
        with Process("", ["pip", "show", package]) as p:
            for line in p.stdout:
                if line.lower().startswith("version: "):
                    return line[9:].strip()
            return None

    def process_one(self, start, end, branch, please_stop):
        # ASSUME PREVIOUS WORK IS DONE
        # UPDATE THE DATABASE STATE
        self.done.min = mo_math.min(end, self.done.min)
        self.done.max = mo_math.max(start, self.done.max)
        self.set_state()

        try:
            pushes = make_push_objects(
                from_date=start.format(), to_date=end.format(), branch=branch
            )
        except MissingDataError:
            return
        except Exception as e:
            raise Log.error("not expected", cause=e)

        Log.note(
            "Found {{num}} pushes on {{branch}} in ({{start}}, {{end}})",
            num=len(pushes),
            start=start,
            end=end,
            branch=branch,
        )

        data = []
        try:
            for push in pushes:
                if please_stop:
                    break

                with Timer("get tasks for push {{push}}", {"push": push.id}):
                    try:
                        schedulers = [
                            label.split("shadow-scheduler-")[1]
                            for label in push.scheduled_task_labels
                            if "shadow-scheduler" in label
                        ]
                    except Exception as e:
                        Log.warning("could not get schedulers", cause=e)
                        schedulers = []

                    scheduler = []
                    for s in schedulers:
                        try:
                            scheduler.append(
                                {
                                    "name": s,
                                    "tasks": jx.sort(
                                        push.get_shadow_scheduler_tasks(s)
                                    ),
                                }
                            )
                        except Exception:
                            pass
                try:
                    regressions = push.get_regressions("label").keys()
                except Exception as e:
                    regressions = []
                    Log.warning(
                        "could not get regressions for {{push}}", push=push.id, cause=e
                    )

                # RECORD THE PUSH
                data.append(
                    {
                        "push": {
                            "id": push.id,
                            "date": push.date,
                            "changesets": push.revs,
                            "backedoutby": push.backedoutby,
                        },
                        "schedulers": scheduler,
                        "regressions": [
                            {"label": name} for name in jx.sort(regressions)
                        ],
                        "branch": branch,
                        "etl": {
                            "revision": git.get_revision(),
                            "timestamp": Date.now(),
                        },
                    }
                )
        finally:
            # ADD WHATEVER WE HAVE
            with Timer("adding {{num}} records to bigquery", {"num": len(data)}):
                self.destination.extend(data)

    def process(self, please_stop):
        done = self.done
        config = self.config

        # ADD CHUNKS OF WORK
        self.todo = []
        if done.max < config.range.max:
            # ADD WORK GOING FORWARDS
            start = Date.floor(done.max, config.interval)
            while start < config.range.max:
                end = start + config.interval
                for branch in config.branches:
                    self.todo.append((start, end, branch))
                start = end
        if config.range.min < done.min:
            # ADD WORK GOING BACKWARDS
            end = Date.ceiling(done.min, config.interval)
            while config.range.min < end:
                start = end - config.interval
                for branch in config.branches:
                    self.todo.append((start, end, branch))
                end = start

        try:
            for start, end, branch in self.todo:
                if please_stop:
                    break
                self.process_one(start, end, branch, please_stop)
        except Exception as e:
            Log.warning("Could not complete the etl", cause=e)
        else:
            self.destination.merge_shards()


def main():
    try:
        config = startup.read_settings()
        constants.set(config.constants)
        Log.start(config.debug)

        # SHUNT PYTHON LOGGING TO MAIN LOGGING
        capture_logging()
        # SHUNT ADR LOGGING TO MAIN LOGGING
        # https://loguru.readthedocs.io/en/stable/api/logger.html#loguru._logger.Logger.add
        capture_loguru()

        if config.taskcluster:
           inject_secrets(config)

        @extend(Configuration)
        def update(self, config):
            """
            Update the configuration object with new parameters
            :param config: dict of configuration
            """
            for k, v in config.items():
                if v != None:
                    self._config[k] = v

            self._config["sources"] = sorted(
                map(os.path.expanduser, set(self._config["sources"]))
            )

            # Use the NullStore by default. This allows us to control whether
            # caching is enabled or not at runtime.
            self._config["cache"].setdefault("stores", {"null": {"driver": "null"}})
            object.__setattr__(self, "cache", CustomCacheManager(self._config))
            for _, store in self._config["cache"]["stores"].items():
                if store.path and not store.path.endswith("/"):
                    # REQUIRED, OTHERWISE FileStore._create_cache_directory() WILL LOOK AT PARENT DIRECTORY
                    store.path = store.path + "/"

        if SHOW_S3_CACHE_HIT:
            s3_get = S3Store._get
            @extend(S3Store)
            def _get(self, key):
                with Timer("get {{key}} from S3", {"key": key}, verbose=False) as timer:
                    output = s3_get(self, key)
                    if output is not None:
                        timer.verbose = True
                    return output

        # UPDATE ADR CONFIGURATION
        with Repeat("waiting for ADR", every="10second"):
            adr.config.update(config.adr)
            # DUMMY TO TRIGGER CACHE
            make_push_objects(
                from_date=Date.today().format(), to_date=Date.now().format(), branch="autoland"
            )

        outatime = Till(seconds=Duration(MAX_RUNTIME).total_seconds())
        outatime.then(lambda: Log.alert("Out of time, exit early"))
        Schedulers(config).process(outatime)
    except Exception as e:
        Log.warning("Problem with etl! Shutting down.", cause=e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()
