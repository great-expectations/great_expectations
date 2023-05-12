import pytest
import responses

from great_expectations.data_context._version_checker import _VersionChecker

# Set to some arbitrary value so tests will continue to work regardless of GX's actual version
_MOCK_PYPI_VERSION = "0.16.8"


@pytest.fixture
def enable_pypi_version_check():
    stashed_version = _VersionChecker._LATEST_GX_VERSION_CACHE
    _VersionChecker._LATEST_GX_VERSION_CACHE = None
    yield
    _VersionChecker._LATEST_GX_VERSION_CACHE = stashed_version


@pytest.mark.parametrize(
    "version,expected,status",
    [
        pytest.param("0.15.0", False, 200, id="outdated"),
        pytest.param(_MOCK_PYPI_VERSION, True, 200, id="up-to-date"),
        # packaging.version should take care of dirty hashes but worth checking against
        pytest.param(
            f"{_MOCK_PYPI_VERSION}+59.g1ff4de04d.dirty",
            True,
            200,
            id="up-to-date local dev",
        ),
        # If we error, we shouldn't raise a warning to the user
        pytest.param(_MOCK_PYPI_VERSION, True, 400, id="bad request"),
    ],
)
@pytest.mark.unit
@responses.activate
def test_check_if_using_latest_gx(
    enable_pypi_version_check, version: str, expected: bool, status: int, caplog
):
    pypi_payload = {"info": {"version": _MOCK_PYPI_VERSION}}
    responses.add(
        responses.GET,
        _VersionChecker._PYPI_GX_ENDPOINT,
        json=pypi_payload,
        status=status,
    )

    checker = _VersionChecker(version)
    actual = checker.check_if_using_latest_gx()

    assert actual is expected
    if not actual:
        assert "upgrade" in caplog.text
