from logging.handlers import RotatingFileHandler
import logging
import os


def setup_logging(log_path='logs/app.log', log_level=logging.INFO, max_bytes=10 * 1024 * 1024, backup_count=10):
    """
    Sets up a rotating file handler for logging.

    :param log_path: Path to the log file, relative to the current working directory. Defaults to 'logs/app.log'.
                     The function does not create the directory if it does not exist.
    :param log_level: Logging level. Defaults to logging.INFO.
    :param max_bytes: Maximum log file size in bytes before it gets rotated. Defaults to 10 MB (10*1024*1024 bytes).
    :param backup_count: Number of backup log files to keep. Defaults to 10.
    :return: Configured logger object of type logging.Logger if the directory exists. Returns None if the directory does not exist.

    This function attempts to create a logger that writes to a specified file, rotating it as it reaches a specified size limit
    and keeping a certain number of backup files. The log entries include timestamp, logger name, severity level, and the message.
    An error is logged if the specified directory for the log file does not exist.
    """
    logger = logging.getLogger('KommatiPara')
    logger.setLevel(log_level)

    log_dir = os.path.dirname(log_path)
    if not os.path.exists(log_dir):
        logger.error(f'Log directory does not exist: {log_dir}. Please create it to enable file logging.')
        return None

    handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger
