"""Tests proving one batch_parameters dict can drive a checkpoint spanning
datasource families that historically disagreed about the value type of numeric
batch parameters (file-based assets wanted zero-padded strings, SQL assets wanted
ints). A checkpoint holding one validation definition per family was unreachable
with any single dict; these tests prove it is now reachable.
"""

from __future__ import annotations

import pathlib
import re
import warnings
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.warnings import GxDeprecationWarning

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )

FILE_DATASOURCE_NAME = "cross family files"
SQL_DATASOURCE_NAME = "cross family sql"
FILE_ASSET_NAME = "taxi files"
SQL_ASSET_NAME = "taxi table"
SQL_TABLE_NAME = "taxi_trips"
BATCHING_REGEX = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
TAXI_DATA_DIR = "tests/test_sets/taxi_yellow_tripdata_samples"

# A single-digit month is deliberate: a file connector's captured group is a
# zero-padded string ("04") while a SQL candidate is an unpadded int (4); textual
# normalization would pass a December reproduction and fail every month 1-9.
INT_BATCH_PARAMETERS = {"year": 2018, "month": 4}
DIGIT_STRING_BATCH_PARAMETERS = {"year": "2018", "month": "04"}


@pytest.fixture
def context() -> AbstractDataContext:
    return gx.get_context(mode="ephemeral")


@pytest.fixture
def sqlite_db_path(tmp_path: pathlib.Path) -> pathlib.Path:
    """A sqlite file under tmp_path with a table holding a date column, so the SQL
    side of the checkpoint needs no network and no external database service."""
    db_path = tmp_path / "cross_family.db"
    engine = sa.create_engine(f"sqlite:///{db_path}")
    df = pd.DataFrame(
        {
            "trip_date": [date(2018, 4, 15), date(2018, 5, 20)],
            "passenger_count": [1, 2],
        }
    )
    df.to_sql(SQL_TABLE_NAME, engine, index=False)
    engine.dispose()
    return db_path


def _build_cross_family_checkpoint(
    context: AbstractDataContext, sqlite_db_path: pathlib.Path
) -> Checkpoint:
    """One context holding a pandas-filesystem datasource and a sqlite datasource,
    one checkpoint holding one validation definition per family."""
    file_asset = context.data_sources.add_pandas_filesystem(
        FILE_DATASOURCE_NAME,
        base_directory=TAXI_DATA_DIR,  # type: ignore [arg-type]
    ).add_csv_asset(name=FILE_ASSET_NAME)
    file_batch_definition = file_asset.add_batch_definition_monthly(
        "monthly files", re.compile(BATCHING_REGEX)
    )

    sql_asset = context.data_sources.add_sqlite(
        SQL_DATASOURCE_NAME, connection_string=f"sqlite:///{sqlite_db_path}"
    ).add_table_asset(SQL_ASSET_NAME, table_name=SQL_TABLE_NAME)
    sql_batch_definition = sql_asset.add_batch_definition_monthly("monthly sql", column="trip_date")

    suite = context.suites.add(
        ExpectationSuite(
            "cross family suite",
            expectations=[gxe.ExpectTableRowCountToBeBetween(min_value=1)],
        )
    )
    file_validation = context.validation_definitions.add(
        ValidationDefinition(name="file validation", data=file_batch_definition, suite=suite)
    )
    sql_validation = context.validation_definitions.add(
        ValidationDefinition(name="sql validation", data=sql_batch_definition, suite=suite)
    )
    return context.checkpoints.add(
        Checkpoint(
            name="cross family checkpoint",
            validation_definitions=[file_validation, sql_validation],
        )
    )


@pytest.mark.filesystem
def test_single_int_dict_drives_file_and_sql_checkpoint(
    context: AbstractDataContext, sqlite_db_path: pathlib.Path
) -> None:
    """A checkpoint holding one file-based and one SQL validation definition, run
    once with a single all-integer batch_parameters dict, executes both validations
    and returns a result for each.

    The failure this replaces is a checkpoint that cannot even be built usefully: no
    single dict satisfies both families' historic per-family type contract. The count
    assertion -- not merely "no exception raised" -- is what pins a partial run that
    silently produces fewer results than validation definitions held by the
    checkpoint; a test that only checked for the absence of an exception would not
    distinguish a full run from a partial one.
    """
    checkpoint = _build_cross_family_checkpoint(context, sqlite_db_path)

    result = checkpoint.run(batch_parameters=INT_BATCH_PARAMETERS)

    assert len(result.run_results) == 2
    assert all(validation_result.success for validation_result in result.run_results.values())
    assert result.success


@pytest.mark.filesystem
def test_digit_string_dict_warns_once_across_validation_definitions(
    context: AbstractDataContext, sqlite_db_path: pathlib.Path
) -> None:
    """The same checkpoint, run with digit-strings instead of ints, still executes
    both validations and succeeds -- and emits exactly one deprecation warning under
    default warning filters, not one per validation definition.

    `filterwarnings = ["error"]` governs the rest of the suite, so default filters
    are simulated here explicitly. The single-warning guarantee no longer depends on
    Python's own per-module warning registry: that registry is invalidated
    process-wide by any unrelated call to warnings.filterwarnings/simplefilter
    happening in between two occurrences of the identical warning, and pandas'
    dtype-casting internals do exactly that while reading the file-based validation
    definition's data. The batch-parameter-normalization module instead maintains
    its own dedup keyed on the user's call site, which is immune to that
    interpreter-level filter-state mutation. The file validation definition is
    intentionally kept first (not reordered to dodge the registry-invalidation
    trigger) so this test proves the guarantee on its merits.
    """
    checkpoint = _build_cross_family_checkpoint(context, sqlite_db_path)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        result = checkpoint.run(batch_parameters=DIGIT_STRING_BATCH_PARAMETERS)

    assert len(result.run_results) == 2
    assert all(validation_result.success for validation_result in result.run_results.values())

    deprecation_warnings = [
        warning for warning in caught if issubclass(warning.category, GxDeprecationWarning)
    ]
    assert len(deprecation_warnings) == 1


@pytest.mark.filesystem
def test_validation_definition_run_accepts_digit_string_batch_parameters(
    context: AbstractDataContext,
) -> None:
    """`ValidationDefinition.run` is a documented entry path in its own right --
    distinct from the checkpoint path exercised above and from the per-asset wiring
    exercised in the file-family unit tests. The integer standardization and its
    deprecation behavior are meant to be a property of the product, not of any one
    code path, so this path needs its own coverage rather than relying on the
    checkpoint tests to exercise it incidentally.

    A digit-string batch parameter succeeds (rather than raising), emits exactly one
    deprecation warning, and `result.meta["batch_parameters"]` preserves the user's
    original strings verbatim -- normalization is a comparison-input concern only, and
    must not leak into what gets recorded back to the user.
    """
    file_asset = context.data_sources.add_pandas_filesystem(
        FILE_DATASOURCE_NAME,
        base_directory=TAXI_DATA_DIR,  # type: ignore [arg-type]
    ).add_csv_asset(name=FILE_ASSET_NAME)
    file_batch_definition = file_asset.add_batch_definition_monthly(
        "monthly files", re.compile(BATCHING_REGEX)
    )
    suite = context.suites.add(
        ExpectationSuite(
            "validation definition suite",
            expectations=[gxe.ExpectTableRowCountToBeBetween(min_value=1)],
        )
    )
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="file validation", data=file_batch_definition, suite=suite)
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        result = validation_definition.run(batch_parameters=DIGIT_STRING_BATCH_PARAMETERS)

    assert result.success
    assert result.meta["batch_parameters"] == DIGIT_STRING_BATCH_PARAMETERS

    deprecation_warnings = [
        warning for warning in caught if issubclass(warning.category, GxDeprecationWarning)
    ]
    assert len(deprecation_warnings) == 1
