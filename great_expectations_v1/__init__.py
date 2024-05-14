# Set up version information immediately
from ._version import get_versions  # isort:skip

__version__ = get_versions()["version"]  # isort:skip

from great_expectations_v1.data_context.migrator.cloud_migrator import CloudMigrator
from great_expectations_v1.expectations.registry import (
    register_core_expectations,
    register_core_metrics,
)

del get_versions  # isort:skip

from great_expectations_v1.core.expectation_suite import ExpectationSuite
from great_expectations_v1.data_context import get_context, project_manager, set_context

# By placing this registry function in our top-level __init__,  we ensure that all
# GX workflows have populated expectation registries before they are used.
#
# Both of the following import paths will trigger this file, causing the registration to occur:
#   import great_expectations_v1 as gx
#   from great_expectations_v1.core import ExpectationSuite, ExpectationConfiguration
register_core_metrics()
register_core_expectations()

rtd_url_ge_version = __version__.replace(".", "_")
