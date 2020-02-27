import subprocess
import time
import signal
import boto3

from great_expectations.data_context.util import file_relative_path

TELEMETRY_TEST_BUCKET = "priv.greatexpectations.telemetry"


def test_signal_handling():
    conn = boto3.client("s3", region_name='us-east-1')
    initial_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))

    p = subprocess.Popen(["python", file_relative_path(__file__, "./instantiate_context_with_telemetry.py")],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(3)
    p.send_signal(signal.SIGINT)
    outs, errs = p.communicate()
    assert "INFO" not in str(outs)
    assert "Done constructing a DataContext." in str(outs)
    assert "KeyboardInterrupt" in str(errs)

    new_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))
    assert new_n_logs == initial_n_logs + 1
