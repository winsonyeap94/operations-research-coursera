"""
Logging Setup

CRITICAL 50
ERROR	 40
WARNING	 30
INFO	 20
DEBUG	 10
OUT      9
END      8
START    7
NOTSET 	 0
"""

import sys
import logging
import logging.config
from pathlib import Path
from datetime import datetime, timedelta

current_time = datetime.now()

Path("logs").mkdir(parents=True, exist_ok=True)
DEFAULT_LOG_FILEPATH = "./logs/py_logs.log"

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True
})

# ============================== Additional Logging Level - OUT ==============================
OUT = 9
logging.addLevelName(9, "OUT")


def output(self, message, *args, **kws):
    if self.isEnabledFor(OUT):
        self._log(OUT, message, args, **kws)  # Yes, logger takes its '*args' as 'args'.


logging.Logger.output = output


# ============================== Additional Logging Level - END ==============================
END = 8
logging.addLevelName(END, "END")


def end(self, message, *args, **kws):
    if self.isEnabledFor(END):
        self._log(END, message, args, **kws)  # Yes, logger takes its '*args' as 'args'.


logging.Logger.end = end


# ============================== Additional Logging Level - START ==============================
START = 7
logging.addLevelName(START, "START")


def start(self, message, *args, **kws):
    if self.isEnabledFor(START):
        self._log(START, message, args, **kws)  # Yes, logger takes its '*args' as 'args'.


logging.Logger.start = start


# ============================== Logger Formatter ==============================
formatter = {
    'brief': logging.Formatter(
        fmt="(%(asctime)s) | %(levelname)-8s | %(module)s: %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    ),
    'precise': logging.Formatter(
        fmt="(%(asctime)s) | %(levelname)-8s| %(module)s - %(funcName)s: %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    )
}


class ErrorCounter(object):
    """Decorator to determine number of calls for a method"""
    def __init__(self, method):
        self.method = method
        self.counter = 0

    def __call__(self, *args, **kwargs):
        self.counter += 1
        return self.method(*args, **kwargs)


class ShutdownHandler(logging.Handler):
    def emit(self, record):
        logging.shutdown()
        sys.exit(1)


class Logger(object):

    def __init__(self, log_filepath=None):

        # Initialisation
        if log_filepath is None:
            log_filepath = DEFAULT_LOG_FILEPATH

        self.logger = logging.getLogger(__name__)
        self.logger.propagate = False

        # Clearing existing handlers if it already exists
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # add standard output stream
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter['brief'])
        self.logger.addHandler(stream_handler)

        # add output file
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_filepath,
            maxBytes=10485760,  # 10MB
            backupCount=20,
            encoding='utf8',
            delay=True
        )
        file_handler.setFormatter(formatter['precise'])
        self.logger.addHandler(file_handler)

        self.logger.setLevel(7)

        self.logger.error = ErrorCounter(self.logger.error)

        self.logger.addHandler(ShutdownHandler(level=50))

    def start(self, message):
        global current_time
        current_time = datetime.now()
        self.logger.start(message)

    def info(self, message):
        self.logger.info(message)

    def output(self, message):
        self.logger.output(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def end(self, args):
        time_delta = datetime.now() - current_time
        time_delta -= timedelta(microseconds=time_delta.microseconds)
        if hasattr(args, 'odb'):
            self.info("Commiting results to output database.")
            args.odb.commit()
            args.odb.close()
        self.logger.end("Finished task {} (total time spent: {})".format(
            args.task.title(), time_delta))

