import pytest
import responses

from great_expectations.data_context._version_checker import _VersionChecker

# Set to some arbitrary value so tests will continue to work regardless of GX's actual version
_MOCK_PYPI_VERSION = "0.16.8"


@pytest.mark.parametrize(
    "version,expected,status",
    [
        pytest.param("0.15.0", False, 200, id="outdated"),
        pytest.param(_MOCK_PYPI_VERSION, True, 200, id="up-to-date"),
        pytest.param(
            "0.16.8+59.g1ff4de04d.dirty", True, 200, id="up-to-date local dev"
        ),
        pytest.param("0.16.8", True, 400, id="bad request"),
    ],
)
@pytest.mark.unit
@responses.activate
def test_check_if_using_latest_gx(version: str, expected: bool, status: int, caplog):
    pypi_payload = {"info": {"version": _MOCK_PYPI_VERSION}}
    responses.add(
        responses.GET,
        "https://pypi.org/pypi/great_expectations/json",
        json=pypi_payload,
        status=status,
    )

    checker = _VersionChecker(version)
    actual = checker.check_if_using_latest_gx()

    assert actual == expected
    if not actual:
        assert "upgrade" in caplog.text
