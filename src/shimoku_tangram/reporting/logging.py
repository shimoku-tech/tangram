from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
import logging

class XRayFilter(logging.Filter):
    def filter(self, record):
        segment = xray_recorder.current_segment()
        if segment:
            record.trace_id = segment.trace_id
        else:
            record.trace_id = None
        return True
    

def init_xray_logger(name: str, level: int | None = None) -> logging.Logger:
    patch_all()
    logger = logging.getLogger(name)

    if level is not None:
        logger.setLevel(level)
    else:
        logger.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [ %(trace_id)s ] - %(message)s"
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.addFilter(XRayFilter())

    logger.addHandler(ch)
    return logger

def init_logger(name: str, level: int | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
        
    if level is not None:
        logger.setLevel(level)
    else:
        logger.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [ No Trace ] - %(message)s"
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def end_logger(logger: logging.Logger) -> None:
    xray_recorder.end_segment()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)