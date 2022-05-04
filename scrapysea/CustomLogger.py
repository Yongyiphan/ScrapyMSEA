import logging


def Set_Custom_Logger(name, logTo, level = logging.DEBUG, propagate = True):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    fHandle = logging.FileHandler(logTo, mode="w")
    logger.propagate = propagate    

    fHandle.setFormatter(formatter)

    logger.addHandler(fHandle)

    return logger