import logging
import json
import uuid

import time

from .s3_logging_handler import S3Handler

DEFAULT_TELEMETRY_BUCKET = "io.greatexpectations.telemetry"
logger = logging.getLogger(__name__)


class TelemetryRecord(object):
    def __init__(self, name, datetime, data_context_id, message, action=None):
        self._record = {
            "name": name,
            "datetime": datetime,
            "data_context_id": data_context_id,
            "message": message,
            "action": action,
        }

    def to_json(self) -> str:
        return json.dumps(self._record)


class DataContextLoggingFilter(logging.Filter):

    def __init__(self, data_context_id):
        self._data_context_id = data_context_id

    def filter(self, record: logging.LogRecord) -> int:
        record.__dict__["data_context_id"] = self._data_context_id
        if record.__dict__["telemetry"] is True:
            return True
        return False


class JsonRecordFormatter(logging.Formatter):
    converter = time.gmtime
    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03dZ"

    def format(self, record):
        telemetry_record = TelemetryRecord(
            name=record.name,
            datetime=self.formatTime(record=record),
            data_context_id=record.__dict__.get("data_context_id"),
            message=record.msg,
            action=record.__dict__.get("action")
        )
        return telemetry_record.to_json()


def initialize_telemetry(enabled=True, data_context_id=None, telemetry_bucket=DEFAULT_TELEMETRY_BUCKET):
    """Initialize the telemetry system."""
    if not enabled:
        logger.info("Telemetry is disabled; skipping initialization.")
        return

    if data_context_id is None:
        data_context_id = str(uuid.uuid4())
    ge_root_logger = logging.getLogger("great_expectations")
    ge_root_logger.setLevel(logging.INFO)
    telemetry_handler = S3Handler("great_expectations", telemetry_bucket, compress=True)
    telemetry_handler.setLevel(level=logging.INFO)
    telemetry_filter = DataContextLoggingFilter(data_context_id=data_context_id)
    telemetry_handler.addFilter(telemetry_filter)
    telemetry_formatter = JsonRecordFormatter()
    telemetry_handler.setFormatter(telemetry_formatter)
    ge_root_logger.addHandler(telemetry_handler)
    return data_context_id
