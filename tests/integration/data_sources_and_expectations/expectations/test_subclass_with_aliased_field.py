"""Reproduction for community issue #10455.

Subclassing an Expectation and adding a Pydantic ``Field`` with an ``alias``
allows the Expectation to be instantiated using the alias, but ``Batch.validate``
fails with::

    pydantic.v1.error_wrappers.ValidationError: 1 validation error for ExpectColumnValuesToStartWith
    regex
      extra fields not permitted (type=value_error.extra)

The roundtrip through ``ExpectationConfiguration.to_domain_obj`` re-instantiates
the subclass using the underlying field name (``regex``) rather than the alias
(``startswith``), and Pydantic v1 rejects the field-name unless the
(v1-specific) ``allow_population_by_field_name`` is enabled. The user reports
that even setting ``Config.populate_by_name = True`` (or ``extra = "allow"``)
does not fix the issue.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.core.suite_parameters import SuiteParameterDict  # noqa: TC001
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch


class ExpectColumnValuesToStartWith(gxe.ExpectColumnValuesToMatchRegex):
    """Pre-fill a regex expectation with a caret, exposing it via an aliased field."""

    regex: str | SuiteParameterDict = pydantic.Field(
        default="(?s).*",
        alias="startswith",
        description="Expect rows in a given column to start with some particular value.",
    )

    @pydantic.validator("regex", pre=True)
    def validate_regex(cls, v: str) -> str:
        return (
            "^"
            + "".join(
                char if char not in set(r"[@_!#$%^&*()<>?/\|}{~:]") else "\\" + char for char in v
            )
            + ".*"
        )

    class Config(gxe.ExpectColumnValuesToMatchRegex.Config):
        populate_by_name = True


DATA = pd.DataFrame({"col2": ["bcc", "bdd", "abc"]})


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_subclass_with_aliased_field_can_be_validated(batch_for_datasource: Batch) -> None:
    """Validating a subclassed expectation with an aliased Pydantic Field should work.

    Expected: validation runs and reports two unexpected rows ("bcc" and "bdd"),
    because only the row "abc" starts with "a".

    Actual (bug): a Pydantic ValidationError is raised inside
    ``Validator.graph_validate`` -> ``ExpectationConfiguration.to_domain_obj``
    because the kwargs passed to the subclass use the underlying field name
    (``regex``) instead of the alias (``startswith``).
    """
    expectation = ExpectColumnValuesToStartWith(column="col2", startswith="a")
    result = batch_for_datasource.validate(expectation)
    assert not result.success
    # The bug surfaced as a captured ValidationError during round-trip; assert
    # validation actually ran end-to-end rather than swallowing an exception.
    assert not result.exception_info["raised_exception"]
