import traceback

from _pytest.logging import LogCaptureFixture
from click.testing import Result

VALIDATION_OPERATORS_DEPRECATION_MESSAGE: str = "Your data context with this configuration version uses validation_operators, which are being deprecated."
LEGACY_CONFIG_DEFAULT_CHECKPOINT_STORE_MESSAGE: str = (
    "Detected legacy config version (2.0) so will try to use "
    "default checkpoint store."
)


def assert_dict_key_and_val_in_stdout(dict_, stdout):
    """Use when stdout contains color info and command chars"""
    for key, val in dict_.items():
        if isinstance(val, dict):
            assert key in stdout
            assert_dict_key_and_val_in_stdout(val, stdout)
        else:
            assert key in stdout
            assert str(val) in stdout


def assert_no_logging_messages_or_tracebacks(
    my_caplog, click_result, allowed_deprecation_message=None
):
    """
    Use this assertion in all CLI tests unless you have a very good reason.

    Without this assertion, it is easy to let errors and tracebacks bubble up
    to users without being detected, unless you are manually inspecting the
    console output (stderr and stdout), as well as logging output from every
    test.

    Usage:

    ```
    def test_my_stuff(caplog):
        ...
        result = runner.invoke(...)
        ...
        assert_no_logging_messages_or_tracebacks(caplog, result)
    ```

    :param my_caplog: the caplog pytest fixutre
    :param click_result: the Result object returned from click runner.invoke()
    :param allowed_deprecation_message: Deprecation message that may be allowed
    """
    if allowed_deprecation_message:
        assert_logging_message_present(
            my_caplog=my_caplog, message=allowed_deprecation_message
        )
    else:
        assert_no_logging_messages(my_caplog)
    assert_no_tracebacks(click_result)


def assert_logging_message_present(my_caplog, message):
    """
    Assert presence of message in logging output messages.

    :param my_caplog: the caplog pytest fixutre
    :param message: message to be searched in caplog
    """
    assert isinstance(
        my_caplog, LogCaptureFixture
    ), "Please pass in the caplog object from your test."
    messages = my_caplog.messages
    assert isinstance(messages, list)
    if messages:
        print("Found logging messages:\n")
        print("\n".join([m for m in messages]))
    assert any([message in element for element in messages])


def assert_no_logging_messages(my_caplog):
    """
    Assert no logging output messages.

    :param my_caplog: the caplog pytest fixutre
    """
    assert isinstance(
        my_caplog, LogCaptureFixture
    ), "Please pass in the caplog object from your test."
    messages = my_caplog.messages
    assert isinstance(messages, list)
    if messages:
        print("Found logging messages:\n")
        print("\n".join([m for m in messages]))
    assert not messages


def assert_no_tracebacks(click_result):
    """
    Assert no tracebacks.

    :param click_result: the Result object returned from click runner.invoke()
    """

    assert isinstance(
        click_result, Result
    ), "Please pass in the click runner invoke result object from your test."
    if click_result.exc_info:
        # introspect the call stack to make sure no exceptions found there way through
        # https://docs.python.org/2/library/sys.html#sys.exc_info
        _type, value, _traceback = click_result.exc_info
        if not isinstance(value, SystemExit):
            # SystemExit is a known "good" exit type
            print("".join(traceback.format_tb(_traceback)))
            assert False, "Found exception of type {} with message {}".format(
                _type, value
            )
    if not isinstance(click_result.exception, SystemExit):
        # Ignore a SystemeExit, because some commands intentionally exit in an error state
        assert not click_result.exception, "Found exception {}".format(
            click_result.exception
        )
    assert (
        "traceback" not in click_result.output.lower()
    ), "Found a traceback in the console output: {}".format(click_result.output)
    assert (
        "traceback" not in click_result.stdout.lower()
    ), "Found a traceback in the console output: {}".format(click_result.stdout)
    try:
        assert (
            "traceback" not in click_result.stderr.lower()
        ), "Found a traceback in the console output: {}".format(click_result.stderr)
    except ValueError as ve:
        # sometimes stderr is not captured separately
        pass
