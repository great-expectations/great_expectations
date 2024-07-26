# isort:skip_file

# Set up version information immediately
from ._version import get_versions as _get_versions

__version__ = _get_versions()["version"]
del _get_versions

# great_expectations.data_context must be imported first or we will have circular dependency issues
import great_expectations.data_context  # isort:skip
import great_expectations.core

from great_expectations.data_context.data_context.context_factory import get_context

# # By placing this registry function in our top-level __init__,  we ensure that all
# # GX workflows have populated expectation registries before they are used.
from great_expectations.expectations.registry import (
    register_core_expectations as _register_core_expectations,
    register_core_metrics as _register_core_metrics,
)

_register_core_metrics()
_register_core_expectations()

del _register_core_metrics
del _register_core_expectations

from great_expectations import exceptions
from great_expectations import expectations
from great_expectations import checkpoint

from great_expectations.checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.validation_definition import ValidationDefinition
