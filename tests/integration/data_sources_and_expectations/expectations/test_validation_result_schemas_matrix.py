"""Matrix runner for validation result schema coverage.

Runs every (expectation x result_format x data_source) cell against the shared fixture frame
published in ``_validation_result_schemas_cases.py`` and writes a structured findings JSON file
recording what each engine actually put in the result dict.

A cell counts as coverage only when it produced a real result dict. Four things can prevent
that, and each one fails the cell rather than filing it:

- ``batch.validate`` raises outright;
- ``batch.validate`` returns, but records a raised exception in ``exception_info`` -- the result
  dict is then empty, which a schema parses happily and which would otherwise be filed as a clean
  parse of nothing at all;
- the result dict is empty at a format that carries one, with nothing raised, and the case has not
  declared that its expectation returns no payload;
- the parsed model does not reproduce the raw dict unchanged.

The one legitimate reason for a cell not to run is a gap the case declares itself, with a reason:
either the expectation has no meaning on that execution engine, or the named data source cannot
evaluate it at all. Both are recorded as ``unsupported`` findings *before* the cell is skipped --
a cell absent from the artifact is indistinguishable from one that was never collected, so a
declared hole has to be filed to read as declared. Nothing else is skipped.

Findings file location (relative to the worktree root):
    tests/_artifacts/validation_result_schemas/findings/<run_id>.json

xdist note: this module uses a session-scoped FindingsWriter; parallelising within a single
session would cause concurrent writes to the same JSON file. The ``no_xdist`` marker documents
this constraint. CI uses ``--dist loadfile`` which naturally routes all cells from this file to a
single worker, so the constraint is satisfied without extra conftest machinery.
"""

from __future__ import annotations

import datetime
import secrets
from typing import TYPE_CHECKING, Callable, Generator, List, Optional

import pytest

from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_result_schemas.dispatcher import ParseError
from great_expectations.core.validation_result_schemas.findings_emitter import (
    FindingsWriter,
)
from great_expectations.core.validation_result_schemas.types import Status
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.expectations._validation_result_schemas_cases import (  # noqa: E501
    EXPECTATION_CASES,
    MATRIX_FIXTURE_DATA,
    ExpectationCase,
)
from tests.integration.data_sources_and_expectations.expectations._validation_result_schemas_helpers import (  # noqa: E501
    assert_field_set_covered,
    release_sql_connections,
    resolve_self_references,
    summarize_raised_exception,
    summarize_raw_dict,
)
from tests.integration.test_utils.data_source_config.tiers import ALL_DATA_SOURCES

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )
    from great_expectations.datasource.fluent.interfaces import Batch
    from tests.integration.test_utils.data_source_config.base import BatchTestSetup

# ---------------------------------------------------------------------------
# Module-level markers
# ---------------------------------------------------------------------------
#
# `no_xdist`: the session-scoped FindingsWriter must not be split across xdist workers. CI uses
# --dist loadfile, which enforces this automatically.
#
# `result_schema_matrix`: names this suite so a run can select or deselect it as a whole. It is
# not one of the required markers -- the harness's parameterization already attaches exactly one
# of those per data source -- so it adds a second name for these tests without changing which
# lane runs them.
pytestmark = [pytest.mark.no_xdist, pytest.mark.result_schema_matrix]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_run_id() -> str:
    """Generate a time-stamped run ID when ``--vrs-run-id`` is not supplied."""
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    # Not the global random module: the test suite reseeds it in fixtures, which would
    # make two runs starting in the same second collide.
    suffix = secrets.token_hex(3)
    return f"{ts}-{suffix}"


def _extra_fields_rejected(exc: BaseException) -> List[str]:
    """Field names pydantic rejected as unpermitted extras, if *exc* is such a failure.

    ``schema_extras_rejected`` is only observable on the FAILED path: a raw key the schema
    does not declare rejects the whole dict with ``Extra.forbid`` before a finding could ever
    be filed as PARSED. This reads the offending keys directly off the wrapped pydantic errors
    ``ParseError`` carries, rather than parsing them back out of the exception's message text.
    Returns an empty list for every other kind of failure -- a metric that raised, a value
    :func:`_helpers.assert_field_set_covered` caught as changed, or a ParseError raised for a
    reason other than an extra field (an unregistered expectation type, for example).
    """
    if not isinstance(exc, ParseError) or exc.pydantic_errors is None:
        return []
    return [
        str(err["loc"][0])
        for err in exc.pydantic_errors
        if err.get("type") == "value_error.extra" and err.get("loc")
    ]


# ---------------------------------------------------------------------------
# Session-scoped findings writer fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def _findings_writer(request: pytest.FixtureRequest) -> Generator[FindingsWriter, None, None]:
    """Session-scoped FindingsWriter; yields writer, flushes on session teardown."""
    run_id: str = request.config.getoption("--vrs-run-id") or _generate_run_id()
    with FindingsWriter(run_id=run_id) as writer:
        yield writer


# ---------------------------------------------------------------------------
# Matrix test
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", EXPECTATION_CASES, ids=lambda c: c.id)
@pytest.mark.parametrize("result_format", list(ResultFormat), ids=lambda f: f.value)
@parameterize_batch_for_data_sources(
    data_source_configs=ALL_DATA_SOURCES,
    data=MATRIX_FIXTURE_DATA,
)
def test_validation_result_schema_matrix(
    batch_for_datasource: Batch,
    _batch_setup_for_datasource: BatchTestSetup,
    case: ExpectationCase,
    result_format: ResultFormat,
    _findings_writer: FindingsWriter,
) -> None:
    """Validate one (expectation x result_format x data_source) cell and record what it produced.

    The engine is read from the data source's own declaration rather than sniffed from the fluent
    datasource ``type`` string. The declaration states the engine as a fact about the data source;
    the type string is a transport name, and string-matching on it silently mis-keyed every data
    source whose type name is neither one of the three engine names nor a listed dialect -- the
    file-backed pandas and Spark configs and the SQL Server one, three of the eleven here. Those
    three would have carried a wrong ``engine_hint`` into every engine-keyed resolution, and their
    findings were filed under an engine name that is not one of the three the findings vocabulary
    admits.
    """
    config = _batch_setup_for_datasource.config
    execution_engine = config.data_source_spec.execution_engine
    assert execution_engine is not None, (
        f"Data source {config.test_id!r} is parameterized into this matrix but declares no "
        "execution engine, so there is no engine to key its findings on."
    )
    engine_hint: str = execution_engine.value
    datasource_test_id: str = config.test_id

    expectation_type: str = case.expectation.expectation_type

    def _write(status: Status, **extra: object) -> None:
        """Record one finding for this cell, with the coordinates every finding carries."""
        _findings_writer.write_finding(
            {
                "expectation_type": expectation_type,
                "result_format": result_format.value,
                "engine": engine_hint,
                "datasource_test_id": datasource_test_id,
                "status": status.value,
                **extra,  # type: ignore[typeddict-item]
            }
        )

    if engine_hint not in case.engines:
        # A declared per-engine gap.  Recorded before skipping for the same reason the
        # per-data-source gap below is: an omitted cell is indistinguishable in the artifact
        # from one that was never collected, so a declared hole would read as missing coverage.
        _write(Status.UNSUPPORTED, error_summary=case.engine_restriction_reason)
        pytest.skip(
            f"[{case.id}][{result_format.value}][{engine_hint}]: {case.engine_restriction_reason}"
        )

    expectation = resolve_self_references(
        case.expectation,
        table_name=getattr(_batch_setup_for_datasource, "table_name", None),
        data_source_name=batch_for_datasource.datasource.name,
    )

    unsupported_reason = case.unsupported_data_sources.get(datasource_test_id)
    if unsupported_reason is not None:
        # A declared per-data-source gap. Recorded so the findings show the hole, skipped so it
        # is not mistaken for a schema that failed to describe a result that was never produced.
        _write(Status.UNSUPPORTED, error_summary=unsupported_reason)
        pytest.skip(
            f"[{case.id}][{result_format.value}][{datasource_test_id}]: {unsupported_reason}"
        )

    try:
        raw_evr = batch_for_datasource.validate(expectation, result_format=result_format)
    except Exception as exc:
        _write(
            Status.FAILED,
            error_summary=f"batch.validate raised: {type(exc).__name__}: {exc}",
        )
        pytest.fail(
            f"[{case.id}][{result_format.value}][{engine_hint}]: "
            f"batch.validate raised {type(exc).__name__}: {exc}"
        )
    finally:
        release_sql_connections(batch_for_datasource)

    raw_result: dict = raw_evr.result or {}
    _reject_vacuous_result(case, result_format, engine_hint, raw_evr, raw_result, _write)

    try:
        typed = raw_evr.as_typed(result_format=result_format, engine_hint=engine_hint)
    except Exception as exc:
        extras_rejected = _extra_fields_rejected(exc)
        _write(
            Status.FAILED,
            **summarize_raw_dict(raw_result),
            error_summary=f"as_typed raised: {type(exc).__name__}: {exc}",
            **({"schema_extras_rejected": extras_rejected} if extras_rejected else {}),
        )
        pytest.fail(
            f"[{case.id}][{result_format.value}][{engine_hint}]: "
            f"as_typed raised {type(exc).__name__}: {exc}"
        )

    # Coverage assertion: every raw key must be reproduced, unchanged, on the parsed model.
    try:
        assert_field_set_covered(raw_result, typed)
    except AssertionError as exc:
        _write(
            Status.FAILED,
            **summarize_raw_dict(raw_result),
            matched_variant=type(typed).__name__,
            error_summary=str(exc),
        )
        pytest.fail(f"[{case.id}][{result_format.value}][{engine_hint}]: {exc}")

    # Success path — record parsed finding. Every raw key is a model field, by the coverage
    # assertion above; the fields worth recording here are the ones the model carries that the
    # raw dict never produced -- what a curator would otherwise have to diff by hand.
    model_dict: dict = typed.dict()
    schema_fields_absent_from_result = [k for k in model_dict if k not in raw_result]

    _write(
        Status.PARSED,
        **summarize_raw_dict(raw_result),
        matched_variant=type(typed).__name__,
        schema_fields_absent_from_result=schema_fields_absent_from_result,
    )


def _reject_vacuous_result(
    case: ExpectationCase,
    result_format: ResultFormat,
    engine_hint: str,
    raw_evr: ExpectationValidationResult,
    raw_result: dict,
    _write: Callable[..., None],
) -> None:
    """Fail the cell if it produced nothing a schema could be measured against."""

    # A metric that raised leaves an empty result dict behind. Every schema in this package
    # accepts an empty dict, so the cell would otherwise parse cleanly and be filed as coverage of
    # a result that was never produced.
    raised_summary: Optional[str] = summarize_raised_exception(raw_evr.exception_info)
    if raised_summary is not None:
        _write(
            Status.FAILED,
            **summarize_raw_dict(raw_result),
            error_summary=f"metric raised: {raised_summary}",
        )
        pytest.fail(
            f"[{case.id}][{result_format.value}][{engine_hint}]: metric raised {raised_summary}. "
            "The cell produced no result dict, so it proves nothing about the schema; "
            "reconfigure the case against the shared fixture frame, or restrict its engines."
        )

    # An empty result dict at a format that is supposed to carry one is the other shape a vacuous
    # cell takes: nothing raised, nothing was computed either, and every schema here accepts `{}`.
    # An expectation that genuinely returns no payload declares that on its case.
    if (
        result_format is not ResultFormat.BOOLEAN_ONLY
        and not raw_result
        and not case.empty_result_reason
    ):
        _write(
            Status.FAILED,
            **summarize_raw_dict(raw_result),
            error_summary="empty result dict at a non-BOOLEAN_ONLY result format",
        )
        pytest.fail(
            f"[{case.id}][{result_format.value}][{engine_hint}]: the result dict is empty at a "
            "format that carries one, so the cell records nothing. Reconfigure the case, or -- if "
            "this expectation really returns no payload on any engine -- say so in its "
            "`empty_result_reason`."
        )
