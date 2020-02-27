import subprocess
import time
import signal
import datetime

import boto3

from great_expectations.data_context.util import file_relative_path

TELEMETRY_TEST_BUCKET = "priv.greatexpectations.telemetry"


def test_telemetry_transmission():
    conn = boto3.client("s3", region_name='us-east-1')
    initial_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))

    p = subprocess.Popen(
        ["python", file_relative_path(__file__, "./instantiate_context_with_telemetry.py"), "0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    outs, errs = p.communicate()
    outs = str(outs)
    errs = str(errs)
    assert "INFO" not in outs
    assert "Done constructing a DataContext" in outs
    assert "Ending a long nap" in outs
    assert "KeyboardInterrupt" not in errs

    new_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))
    assert new_n_logs == initial_n_logs + 1


def test_send_completes_on_kill():
    """Test that having telemetry enabled does not negatively impact kill signals or cause loss of queued telemetry. """

    # Establish baseline
    conn = boto3.client("s3", region_name='us-east-1')
    initial_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))

    # Execute process that initializes data context
    acceptable_startup_time = 6
    acceptable_shutdown_time = 1
    nap_time = 30
    start = datetime.datetime.now()
    # Instruct the process to wait for 30 seconds after initializing before completing.
    p = subprocess.Popen(
        ["python", file_relative_path(__file__, "./instantiate_context_with_telemetry.py"),
         str(nap_time), "True", "False"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    time.sleep(acceptable_startup_time)
    # Ensure that our logs have not yet been written; we are waiting for a time or volume to use to chunk the logs
    intermediate_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))
    assert initial_n_logs == intermediate_n_logs

    # Send a signal to kill
    p.send_signal(signal.SIGINT)
    outs, errs = p.communicate()
    end = datetime.datetime.now()

    # Ensure that the process shut down earlier than it would have
    assert datetime.timedelta(
        seconds=acceptable_startup_time
    ) < (end - start) < datetime.timedelta(
        seconds=acceptable_startup_time + acceptable_shutdown_time)

    outs = str(outs)
    errs = str(errs)
    assert "INFO" not in outs
    assert "Done constructing a DataContext" in outs
    assert "Ending a long nap" not in outs
    assert "KeyboardInterrupt" in errs

    new_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))
    assert new_n_logs == initial_n_logs + 1


def test_graceful_failure_with_no_internet():
    """Test that having telemetry enabled does not negatively impact kill signals or cause loss of queued telemetry. """

    # Establish baseline
    conn = boto3.client("s3", region_name='us-east-1')
    initial_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))

    # Execute process that initializes data context
    # NOTE - JPC - 20200227 - this is crazy long (not because of logging I think, but worth revisiting)
    acceptable_startup_time = 6
    acceptable_shutdown_time = 1
    nap_time = 0
    start = datetime.datetime.now()
    # Instruct the process to wait for 30 seconds after initializing before completing.
    p = subprocess.Popen(
        ["python", file_relative_path(__file__, "./instantiate_context_with_telemetry.py"),
         str(nap_time), "True", "True"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    outs, errs = p.communicate()
    end = datetime.datetime.now()
    # We didn't wait or send a signal, so just check that times were reasonable
    assert (end - start) < datetime.timedelta(
        seconds=acceptable_startup_time + acceptable_shutdown_time)
    outs = str(outs)
    errs = str(errs)
    assert "INFO" not in outs
    assert "Done constructing a DataContext" in outs
    assert "Ending a long nap" in outs
    assert "Unable to open S3 Stream. Telemetry will be disabled" in errs

    new_n_logs = len(conn.list_objects(Bucket=TELEMETRY_TEST_BUCKET).get("Contents", []))
    # Nothing should have been written because we blocked internet
    assert new_n_logs == initial_n_logs
