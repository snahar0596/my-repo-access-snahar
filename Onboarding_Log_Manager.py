import logging
import sys

# 2024-01-15 10:30:45,131 - WARNING - my_app:script1.py:18 - Some message
FORMATTER = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(name)s:%(module)s:%(lineno)d - %(message)s')
ROOT_CONFIGURED = False


def create_console_handler():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(FORMATTER)
    handler.setLevel(logging.INFO)
    return handler


def setup_root_logger():
    global ROOT_CONFIGURED

    if ROOT_CONFIGURED:
        return

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    root_handler = create_console_handler()
    root_logger.addHandler(root_handler)

    ROOT_CONFIGURED = True


def get_module_logger(module_name: str):
    if not ROOT_CONFIGURED:
        setup_root_logger()

    logger = logging.getLogger(module_name)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(create_console_handler())

    return logger


# import Onboarding_Log_Manager
# logger = Onboarding_Log_Manager.get_module_logger('OCRMod')