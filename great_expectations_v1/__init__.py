# isort:skip_file

# Set up version information immediately
from ._version import get_versions as _get_versions

__version__ = _get_versions()["version"]
del _get_versions

# great_expectations.data_context must be imported first or we will have circular dependency issues
import great_expectations_v1.data_context  # isort:skip
import great_expectations_v1.core

from great_expectations_v1.data_context.data_context.context_factory import get_context

# # By placing this registry function in our top-level __init__,  we ensure that all
# # GX workflows have populated expectation registries before they are used.
from great_expectations_v1.expectations.registry import (
    register_core_expectations as _register_core_expectations,
    register_core_metrics as _register_core_metrics,
)

_register_core_metrics()
_register_core_expectations()

del _register_core_metrics
del _register_core_expectations

from great_expectations_v1 import exceptions
from great_expectations_v1 import expectations
from great_expectations_v1 import checkpoint

from great_expectations_v1.checkpoint import Checkpoint
from great_expectations_v1.core.expectation_suite import ExpectationSuite
from great_expectations_v1.core.result_format import ResultFormat
from great_expectations_v1.core.run_identifier import RunIdentifier
from great_expectations_v1.core.validation_definition import ValidationDefinition
