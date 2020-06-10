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

import logging
from sys import stderr

import loguru

from mo_dots import DataObject
from mo_future import is_text
from mo_logs import machine_metadata, Log, Except

LOG_LEVEL = logging.INFO


def capture_loguru():
    """
    CAPTURE LOGGING FROM loguru
    """
    loguru.logger.remove()
    loguru.logger.add(
        _loguru_emit, level="NOTSET", format="{message}", filter=lambda r: True,
    )


def capture_logging():
    """
    CAPTURE LOGGING FROM PYTHON LOGGING
    """
    logger = logging.getLogger()
    logger.setLevel(level=logging.NOTSET)
    logger.addHandler(_LogHanlder(logger))


def _loguru_emit(message):
    record = message.record
    if record["level"].no < LOG_LEVEL:
        return
    record["machine"] = machine_metadata
    log_format = '{{machine.name}} (pid {{process}}) - {{time|datetime}} - {{thread}} - "{{file}}:{{line}}" - ({{function}}) - {{message}}'
    Log.main_log.write(log_format, record)


class _LogHanlder(logging.Handler):
    def __init__(self, logger):
        logging.Handler.__init__(self)
        self.logger = logger

    def emit(self, record):
        if record.levelno < LOG_LEVEL:
            return

        try:
            if record.args and is_text(record.msg):
                message = record.msg % tuple(record.args)
            else:
                message = record.msg
            record = DataObject(record)
            record.machine = machine_metadata
            record.message = message
            log_format = '{{machine.name}} (pid {{process}}) - {{created|datetime}} - {{threadName}} - "{{pathname}}:{{lineno}}" - ({{funcName}}) - {{levelname}} - {{message}}'
            Log.main_log.write(log_format, record)
        except Exception as e:
            e = Except.wrap(e)
            stderr.write("problem with logging")
