from logging.handlers import RotatingFileHandler
import logging
import os


def setup_logging():
    logger = logging.getLogger('KommatiPara')
    logger.setLevel(logging.INFO)

    # Define a rotating file handler
    handler = RotatingFileHandler(
        f'{os.getcwd()}/app/logs/app.log',
        maxBytes=10 * 1024 * 1024,  # Log file size limit (10 MB)
        backupCount=10,  # Number of backup files to keep
    )

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger
