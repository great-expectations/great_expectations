import pytest

from great_expectations.validator.exception_info import ExceptionInfo


@pytest.fixture
def exception_info() -> ExceptionInfo:
    return ExceptionInfo(
        exception_traceback="my exception traceback",
        exception_message="my exception message",
        raised_exception=True,
    )


@pytest.mark.unit
def test_exception_info__eq__and__ne__(exception_info: ExceptionInfo) -> None:
    other_exception_info = ExceptionInfo(
        exception_traceback="", exception_message="", raised_exception=True
    )
    assert exception_info != other_exception_info


@pytest.mark.unit
def test_exception_info__repr__(exception_info: ExceptionInfo) -> None:
    assert (
        exception_info.__repr__()
        == "{'exception_traceback': 'my exception traceback', 'exception_message': 'my exception message', 'raised_exception': True}"
    )


@pytest.mark.unit
def test_exception_info__str__(exception_info: ExceptionInfo) -> None:
    assert (
        exception_info.__str__()
        == '{\n  "exception_traceback": "my exception traceback",\n  "exception_message": "my exception message",\n  "raised_exception": true\n}'
    )
