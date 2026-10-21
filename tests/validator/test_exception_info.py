from unittest import mock

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
@pytest.mark.parametrize("other", [None, "x", 5, object()], ids=["none", "str", "int", "object"])
def test_exception_info_is_unequal_to_a_non_exception_info(
    exception_info: ExceptionInfo, other: object
) -> None:
    """Comparing with another type must not read `NotImplemented` as a truth value.

    Through Python 3.13 `!=` reported such an operand as equal; Python 3.14 raises.
    """
    assert (exception_info == other) is False
    assert exception_info != other


@pytest.mark.unit
def test_exception_info_is_not_unequal_to_an_identical_exception_info(
    exception_info: ExceptionInfo,
) -> None:
    same = ExceptionInfo(
        exception_traceback="my exception traceback",
        exception_message="my exception message",
        raised_exception=True,
    )
    assert exception_info == same
    assert not (exception_info != same)


# `ExpectationValidationResult.exception_info` defaults to a plain dict, and results built by the
# validator carry `ExceptionInfo` values, so the library compares the two. `ExceptionInfo` declines
# a plain dict, and Python then defers to `dict` comparison, which compares contents.
@pytest.mark.unit
@pytest.mark.parametrize(
    ("traceback", "expected_equal"),
    [
        pytest.param("my exception traceback", True, id="same-contents"),
        pytest.param("another traceback", False, id="different-contents"),
    ],
)
def test_exception_info_compares_with_a_plain_dict_by_contents(
    exception_info: ExceptionInfo, traceback: str, expected_equal: bool
) -> None:
    plain = {
        "exception_traceback": traceback,
        "exception_message": "my exception message",
        "raised_exception": True,
    }

    assert (exception_info == plain) is expected_equal
    assert (exception_info != plain) is not expected_equal


@pytest.mark.unit
def test_exception_info_defers_to_an_operand_that_equals_anything(
    exception_info: ExceptionInfo,
) -> None:
    assert exception_info == mock.ANY
    assert not (exception_info != mock.ANY)


@pytest.mark.unit
def test_exception_info__repr__(exception_info: ExceptionInfo) -> None:
    assert (
        exception_info.__repr__()
        == "{'exception_traceback': 'my exception traceback', 'exception_message': 'my exception message', 'raised_exception': True}"  # noqa: E501 # FIXME CoP
    )


@pytest.mark.unit
def test_exception_info__str__(exception_info: ExceptionInfo) -> None:
    assert (
        exception_info.__str__()
        == '{\n  "exception_traceback": "my exception traceback",\n  "exception_message": "my exception message",\n  "raised_exception": true\n}'  # noqa: E501 # FIXME CoP
    )
