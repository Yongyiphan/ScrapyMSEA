import logging
import os
from pathlib import Path

def Set_Custom_Logger(name, logTo, level = logging.DEBUG, propagate = True):
    currentPath = Path(__file__).parent.parent.resolve().as_posix()
    LogPath = currentPath + "\Logs"
    if not os.path.exists(LogPath):
        os.makedirs(LogPath)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    #formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    formatter = logging.Formatter(fmt='%(message)s')
    fHandle = logging.FileHandler(logTo, mode="w")
    logger.propagate = propagate    

    fHandle.setFormatter(formatter)

    logger.addHandler(fHandle)

    return logger