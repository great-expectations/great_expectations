from _pytest.logging import LogCaptureFixture
from click.testing import Result


def assert_no_logging_messages_or_tracebacks(my_caplog, click_result):
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
    """
    assert isinstance(
        my_caplog, LogCaptureFixture
    ), "Please pass in the caplog object from your test."
    assert isinstance(
        click_result, Result
    ), "Please pass in the click runner invoke result object from your test."

    messages = my_caplog.messages
    assert isinstance(messages, list)
    if messages:
        print(messages)
    assert not messages

    assert (
        "traceback" not in click_result.output.lower()
    ), "Found a traceback in the console output: {}".format(click_result.output)
    assert (
        "traceback" not in click_result.stdout.lower()
    ), "Found a traceback in the console output: {}".format(click_result.stdout)
