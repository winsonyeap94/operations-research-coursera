from .logger import Logger
from .loguru_logger import LoguruLogger
from .cli_parser import BaseParser, process_args

# Creating a generic logger to be used by any script
loguru_logger = LoguruLogger.make_logger()
