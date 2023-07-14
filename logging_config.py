import logging
import os
import time


def initialise_logger(output_log_path):
    try:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        log_folder = output_log_path
        os.makedirs(log_folder, exist_ok=True)
        log_file = os.path.join(log_folder, "eks_monitoring.log")
        logging.basicConfig(
            filename=log_file,
            filemode="w",
            level=logging.DEBUG,
            format="%(asctime)s,%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d:%H:%M:%S",
        )
        logger.info("created at " + str(time.ctime()))
        return logger
    except Exception as ex:
        print(ex)
        print("Failed to create log file: might be due to invalid path")
        exit(1)
