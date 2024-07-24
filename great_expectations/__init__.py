# isort:skip_file

# Set up version information immediately
from ._version import get_versions  # isort:skip

__version__ = get_versions()["version"]  # isort:skip

# This brings in compatibility, data_context, exceptions, get_context
from great_expectations.data_context.data_context.context_factory import get_context
import great_expectations.core

# Checkpoint must be imported after get_context and great_expectations.core
from great_expectations.checkpoint.checkpoint import Checkpoint

# from great_expectations.data_context.migrator.cloud_migrator import CloudMigrator
# from great_expectations.expectations.registry import (
#     register_core_expectations,
#     register_core_metrics,
# )

del get_versions  # isort:skip

# from great_expectations.core.expectation_suite import ExpectationSuite
# from great_expectations import get_context, project_manager, set_context
#
# # By placing this registry function in our top-level __init__,  we ensure that all
# # GX workflows have populated expectation registries before they are used.
# #
# # Both of the following import paths will trigger this file, causing the registration to occur:
# #   import great_expectations as gx
# #   from great_expectations.core.expectation_suite import ExpectationSuite
# #   from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
# register_core_metrics()
# register_core_expectations()

# rtd_url_ge_version = __version__.replace(".", "_")
