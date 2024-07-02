from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all
import logging
import os

patch_all()

class XRayFilter(logging.Filter):
    def filter(self, record):
        segment = xray_recorder.current_segment()
        
        if segment:
            record.trace_id = segment.trace_id
            record.segment_id = segment.id
        else:
            record.trace_id = None
            record.segment_id = None
        return True

def init_logger(name: str, level: int | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    segment = xray_recorder.current_segment()

    trace_id = os.getenv("AWS_XRAY_TRACE_ID", None)
    segment_id = os.getenv("AWS_XRAY_SEGMENT_ID", None)

    if trace_id:
        if segment_id:
            xray_recorder.begin_segment(segment_id, traceid=trace_id)
        else:
            xray_recorder.begin_segment(name, traceid=trace_id)

    if not segment:
        xray_recorder.begin_segment(name)
        
    if level is not None:
        logger.setLevel(level)
    else:
        logger.setLevel(logging.ERROR)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(trace_id)s | %(segment_id)s] - %(message)s"
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.addFilter(XRayFilter())

    logger.addHandler(ch)
    return logger



