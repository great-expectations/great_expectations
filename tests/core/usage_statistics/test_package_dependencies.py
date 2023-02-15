from typing import List

import pytest

from great_expectations.core.usage_statistics.package_dependencies import GXDependencies


def test__get_dependency_names():
    """Test that the regex in _get_dependency_names() parses a requirements file correctly."""
    mock_dependencies: List[str] = [
        "# This is a comment",
        "#This is another comment",
        "--requirement requirements.txt",
        "latest-package",
        "duplicate-package",
        "duplicate-package",  # Should not be filtered in this method
        "pinned-package1==0.2.4",
        "pinned-package2==0.2.4 # With comment after",
        "lower-bound-package1>=5.2.4",
        "lower-bound-package2>=5.2.4 # With comment after",
        "lower-bound-package3>5.2.4",
        "upper-bound-package1<=2.3.8",
        "upper-bound-package2<=2.3.8 # With comment after",
        "upper-bound-package3<2.3.8",
        "two-bounds-package1>=0.8.4,<2.0.0",
        "two-bounds-package2>=0.8.4,<2.0.0 # With comment after",
        "package_with_underscores",
        "1",
        "-",
        "",
    ]
    expected_dependendencies: List[str] = [
        "latest-package",
        "duplicate-package",
        "duplicate-package",
        "pinned-package1",
        "pinned-package2",
        "lower-bound-package1",
        "lower-bound-package2",
        "lower-bound-package3",
        "upper-bound-package1",
        "upper-bound-package2",
        "upper-bound-package3",
        "two-bounds-package1",
        "two-bounds-package2",
        "package_with_underscores",
        "1",
        "-",
    ]
    ge_dependencies = GXDependencies()
    observed_dependencies = ge_dependencies._get_dependency_names(mock_dependencies)
    assert observed_dependencies == expected_dependendencies


@pytest.mark.integration
def test_required_dependency_names_match_requirements_file():
    """If there is a mismatch, GXDependencies should change to match our requirements.txt file.

    See GXDependencies for a utility to check for a mismatch.
    """
    ge_dependencies = GXDependencies()
    assert (
        ge_dependencies.get_required_dependency_names()
        == ge_dependencies.get_required_dependency_names_from_requirements_file()
    )


@pytest.mark.integration
def test_dev_dependency_names_match_requirements_file():
    """If there is a mismatch, GXDependencies should change to match our requirements-dev*.txt files.

    See GXDependencies for a utility to check for a mismatch.
    """
    ge_dependencies = GXDependencies()
    assert ge_dependencies.get_dev_dependency_names() == set(
        ge_dependencies.get_dev_dependency_names_from_requirements_file()
    ) - set(GXDependencies.GX_DEV_DEPENDENCIES_EXCLUDED_FROM_TRACKING)
