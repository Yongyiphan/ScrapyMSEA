import logging
import os
from pathlib import Path
from scrapy.logformatter import LogFormatter
import utils


def Set_Custom_Logger(name, logTo, level=logging.DEBUG, propagate=True):
    # currentPath = Path(__file__).parent.parent.resolve().as_posix()
    Root = utils.Get_ProjectRoot()
    print(Root)
    LogPath = os.path.join(Root, "Logs")
    if not os.path.exists(LogPath):
        os.makedirs(LogPath)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    # formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    formatter = logging.Formatter(fmt="%(message)s")
    LogFile = os.path.join(LogPath, logTo)
    fHandle = logging.FileHandler(LogFile, mode="w")
    logger.propagate = propagate

    fHandle.setFormatter(formatter)

    logger.addHandler(fHandle)

    return logger


class QuietLogFormatter(LogFormatter):
    def scraped(self, item, response, spider):
        return (
            super().scraped(item, response, spider)
            if spider.settings.getbool("LOG_SCRAPED_ITEMS")
            else None
        )
