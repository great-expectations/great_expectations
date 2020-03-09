import pytest

import subprocess
import time
import signal
import datetime
import requests
from dateutil.parser import parse

import boto3

from great_expectations.data_context.util import file_relative_path

TELEMETRY_TEST_URL = "https://mn3i9tgx0b.execute-api.us-east-1.amazonaws.com/test/great_expectations/v1/telemetry"

logGroupName = "/great_expectations/telemetry"


@pytest.fixture(scope="session")
def logstream():
    client = boto3.client('logs', region_name='us-east-1')
    # Warm up a logstream
    logStreamName = None
    requests.post(TELEMETRY_TEST_URL, json={"message": "warmup"})
    attempts = 0
    while attempts < 3:
        attempts += 1
        time.sleep(2)
        logStreams = client.describe_log_streams(
            logGroupName=logGroupName,
            orderBy='LastEventTime'
        )
        lastEventTimestamp = logStreams["logStreams"][-1].get("lastEventTimestamp")
        if lastEventTimestamp is not None:
            lastEvent = datetime.datetime.fromtimestamp(lastEventTimestamp / 1000)
            if (lastEvent - datetime.datetime.now()) < datetime.timedelta(seconds=30):
                logStreamName = logStreams["logStreams"][-1]["logStreamName"]
                break
    if logStreamName is None:
        assert False, "Unable to warm up a log stream for integration testing."
    return client, logStreamName


def test_telemetry_transmission(logstream):
    client, logStreamName = logstream
    pre_events = client.get_log_events(
        logGroupName=logGroupName,
        logStreamName=logStreamName,
        limit=100,
    )
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

    # Wait a bit for the log events to post
    time.sleep(5)
    post_events = client.get_log_events(
        logGroupName=logGroupName,
        logStreamName=logStreamName,
        limit=100,
    )
    assert len(pre_events["events"]) + 2 == len(post_events["events"])


def test_send_completes_on_kill(logstream):
    client, logStreamName = logstream
    pre_events = client.get_log_events(
        logGroupName=logGroupName,
        logStreamName=logStreamName,
        limit=100,
    )
    """Test that having telemetry enabled does not negatively impact kill signals or cause loss of queued telemetry. """
    # Execute process that initializes data context
    acceptable_startup_time = 6
    acceptable_shutdown_time = 1
    nap_time = 30
    start = datetime.datetime.now()
    # Instruct the process to wait for 30 seconds after initializing before completing.
    p = subprocess.Popen(
        ["python", file_relative_path(__file__, "./instantiate_context_with_telemetry.py"),
         str(nap_time), "False", "True"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    time.sleep(acceptable_startup_time)

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
    time.sleep(5)
    post_events = client.get_log_events(
        logGroupName=logGroupName,
        logStreamName=logStreamName,
        limit=100,
    )
    assert len(pre_events["events"]) + 2 == len(post_events["events"])


def test_graceful_failure_with_no_internet():
    """Test that having telemetry enabled does not negatively impact kill signals or cause loss of queued telemetry. """

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
