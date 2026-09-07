from __future__ import annotations

import ast
import importlib
import subprocess
import sys
import tempfile
from dataclasses import FrozenInstanceError, dataclass, field, fields, replace
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

import pytest

from great_expectations.compatibility.typing_extensions import override
from great_expectations.expectations.metadata_types import SupportedDataSources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS,
    JUST_PANDAS_DATA_SOURCES,
    NON_SQL_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    ALL_DATA_SOURCES,
    CURATED_SQL_DATA_SOURCES,
    PANDAS_DATA_SOURCES,
    SPARK_DATA_SOURCES,
    SQL_DATA_SOURCES,
    BigQueryDatasourceTestConfig,
    DatabricksDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PandasDataFrameDatasourceTestConfig,
    PandasFilesystemCsvDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SingleStoreDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
    SqliteDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
    data_sources_for_tier_case,
)
from tests.integration.test_utils.data_source_config import (
    data_source_spec as data_source_spec_module,
)
from tests.integration.test_utils.data_source_config.backend_spec import SqlBackendSpec
from tests.integration.test_utils.data_source_config.base import (
    BatchTestSetup,
    DataSourceTestConfig,
)
from tests.integration.test_utils.data_source_config.data_source_spec import (
    CiLaneRef,
    DataSourceProvisioning,
    DataSourceSpec,
    ExecutionEngineKind,
    MarkerScope,
    SupportTier,
)
from tests.integration.test_utils.data_source_config.registry import (
    _OUTSIDE_SHARED_PARAMETERIZATION,
    RegisteredDataSource,
    data_source_configs_for_engine,
    data_source_configs_for_tier,
    isolated_registry,
    iter_data_source_configs,
    iter_data_source_specs,
    iter_data_sources,
    register_data_source,
    register_sql_config,
)
from tests.integration.test_utils.data_source_config.sql_config import SqlDatasourceTestConfig

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from tests.integration.test_utils.data_source_config.sql import SessionSQLEngineManager

pytestmark = pytest.mark.project


def _make_spec(**overrides: object) -> SqlBackendSpec:
    defaults: dict[str, object] = dict(
        label="throwaway",
        public_name="Throwaway",
        marker="throwaway",
        provisioning=DataSourceProvisioning.LOCAL_FILE,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="throwaway"),
        uses_schema=False,
    )
    defaults.update(overrides)
    return SqlBackendSpec(**defaults)  # type: ignore[arg-type]


# The shared canonical expectation parameterization criterion, as a throwaway record declares it.
# A throwaway record that carries a config class and names an execution engine is subject to the
# mandatory-declaration rule exactly as a real one is, so the records below that do both declare
# the criterion rather than being exempted; the rule itself is proven further down, with records
# built to violate it.
_CANONICAL_CLAIM = frozenset({SupportTier.CANONICAL_EXPECTATIONS})


def _make_config_class(name: str, spec: SqlBackendSpec) -> type:
    return type(name, (), {"DATA_SOURCE_SPEC": spec})


def _make_core_spec(**overrides: object) -> DataSourceSpec:
    """A throwaway core record: no dialect facts, so it can never carry a config class."""
    defaults: dict[str, object] = dict(
        label="throwaway-core",
        public_name="Throwaway Core",
        provisioning=DataSourceProvisioning.IN_PROCESS,
    )
    defaults.update(overrides)
    return DataSourceSpec(**defaults)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _snapshot_registry() -> Iterator[None]:
    """Wrap every test in this module in the registry's snapshot/restore seam.

    The registry is process-global, so a throwaway registration in one test must not survive to
    the next test, and must not survive into the real registry that the wiring drift check and
    other consumers rely on.
    """
    with isolated_registry():
        yield


class TestSqlBackendSpecMarkRoundTrip:
    def test_pytest_mark_round_trips_marker_name_to_literal_mark(self) -> None:
        spec = _make_spec(marker="mysql")

        assert spec.pytest_mark == pytest.mark.mysql

    def test_pytest_mark_round_trips_a_different_marker_name(self) -> None:
        spec = _make_spec(marker="singlestore", tiers=frozenset({SupportTier.CURATED_SQL}))

        assert spec.pytest_mark == pytest.mark.singlestore


class TestSqlBackendSpecTableSchemaItemsDefault:
    def test_spec_with_no_table_schema_item_factory_reports_it_as_absent(self) -> None:
        spec = _make_spec()

        assert spec.table_schema_items is None


class TestIsolatedSnapshotEmptyRegistryCase:
    def test_registry_is_empty_within_a_fresh_isolated_snapshot(self) -> None:
        with isolated_registry():
            assert iter_data_source_configs() == ()


class TestIsolatedSnapshotRestoresRealRegistry:
    def test_registering_a_throwaway_does_not_survive_the_snapshot(self) -> None:
        # Establish a populated baseline inside the module's own autouse isolation, so this test
        # proves both halves of the seam: that entering it clears down to empty, and that exiting
        # it restores exactly what was there beforehand — regardless of what the real registry
        # elsewhere happens to hold.
        register_sql_config(_make_config_class("Baseline", _make_spec(label="baseline")))
        before = iter_data_source_configs()
        assert before != ()

        with isolated_registry():
            assert iter_data_source_configs() == ()
            register_sql_config(_make_config_class("Throwaway", _make_spec()))
            assert iter_data_source_configs() != before

        assert iter_data_source_configs() == before


class TestRegisterSqlConfigOrdering:
    def test_iter_data_source_configs_orders_registrations_by_label_not_registration_order(
        self,
    ) -> None:
        zebra = _make_config_class("Zebra", _make_spec(label="zebra", marker="zebra_marker"))
        apple = _make_config_class("Apple", _make_spec(label="apple", marker="apple_marker"))

        register_sql_config(zebra)
        register_sql_config(apple)

        assert iter_data_source_configs() == (apple, zebra)


class TestDataSourceConfigsForTier:
    def test_returns_only_backends_declaring_the_tier_ordered_by_label(self) -> None:
        member = _make_config_class(
            "Member",
            _make_spec(
                label="member",
                marker="member_marker",
                tiers=frozenset({SupportTier.CURATED_SQL}),
            ),
        )
        non_member = _make_config_class(
            "NonMember", _make_spec(label="non-member", marker="non_member_marker")
        )

        register_sql_config(member)
        register_sql_config(non_member)

        assert data_source_configs_for_tier(SupportTier.CURATED_SQL) == (member,)


class TestDataSourcesForTierCase:
    """`data_sources_for_tier_case` is the one place a backend's `tier_case_exclusions` entry
    takes effect: it returns a tier's members, instantiated in label order, omitting only those
    declaring an exclusion for the given case key.
    """

    def test_omits_only_the_backend_excluding_the_case_and_keeps_the_rest_for_other_keys(
        self,
    ) -> None:
        # Registered zebra before apple - non-alphabetical - so a result in label order can only
        # come from `data_source_configs_for_tier`'s own label sort, never from registration order.
        zebra = _make_config_class(
            "Zebra",
            _make_spec(
                label="zebra",
                marker="zebra_marker",
                tiers=frozenset({SupportTier.CURATED_SQL}),
                tier_case_exclusions={"flaky_case": "observed non-determinism, see issue #1"},
            ),
        )
        apple = _make_config_class(
            "Apple",
            _make_spec(
                label="apple",
                marker="apple_marker",
                tiers=frozenset({SupportTier.CURATED_SQL}),
            ),
        )
        register_sql_config(zebra)
        register_sql_config(apple)

        excluded_case = data_sources_for_tier_case(SupportTier.CURATED_SQL, "flaky_case")
        assert [type(config) for config in excluded_case] == [apple]

        other_case = data_sources_for_tier_case(SupportTier.CURATED_SQL, "unrelated_case")
        assert [type(config) for config in other_case] == [apple, zebra]

    def test_with_no_exclusions_declared_matches_the_tiers_call_time_membership(self) -> None:
        """The behavior-preservation oracle for this accessor, stated call-time-to-call-time
        rather than against `CURATED_SQL_DATA_SOURCES`.

        `data_source_configs_for_tier` reads the registry fresh on every call.
        `CURATED_SQL_DATA_SOURCES` is a list built once, when the defining module is first
        imported, from whatever the
        registry held at that moment. This module's autouse fixture clears the registry around
        every test, so inside a test body a call-time read and that import-time snapshot are
        answering two different questions: comparing them here would pass vacuously today (both
        happen to be empty, since nothing yet declares the curated tier at import time) and would
        fail for a reason that has nothing to do with this accessor the first time a real backend
        joins that tier here without also being re-imported. Comparing `data_sources_for_tier_case`
        to `data_source_configs_for_tier` instead keeps both sides of the comparison call-time,
        so the assertion is meaningful inside this isolated registry and stays correct regardless
        of what
        the real, outside-the-seam registry holds at any given moment. The published-key,
        `CURATED_SQL_DATA_SOURCES`-referencing form of this same oracle belongs in the curated
        suite's own module, which runs against the real, unmodified registry rather than this
        isolated one.
        """
        zebra = _make_config_class(
            "Zebra",
            _make_spec(
                label="zebra", marker="zebra_marker", tiers=frozenset({SupportTier.CURATED_SQL})
            ),
        )
        apple = _make_config_class(
            "Apple",
            _make_spec(
                label="apple", marker="apple_marker", tiers=frozenset({SupportTier.CURATED_SQL})
            ),
        )
        register_sql_config(zebra)
        register_sql_config(apple)

        result = data_sources_for_tier_case(SupportTier.CURATED_SQL, "arbitrary_case")

        assert [type(config) for config in result] == list(
            data_source_configs_for_tier(SupportTier.CURATED_SQL)
        )

    def test_filters_the_tier_it_is_asked_for_rather_than_a_fixed_one(self) -> None:
        """Exclusion is a property of tiers in general, so the accessor has to honour whichever
        tier it is given. Without this, an implementation that ignored its `tier` argument and
        always read one particular tier would satisfy every other test in this class, because
        they all happen to ask about the same tier.
        """
        curated = _make_config_class(
            "Curated",
            _make_spec(
                label="curated-only",
                marker="curated_only_marker",
                tiers=frozenset({SupportTier.CURATED_SQL}),
            ),
        )
        standard = _make_config_class(
            "Standard",
            _make_spec(
                label="standard-only",
                marker="standard_only_marker",
                tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS}),
                tier_case_exclusions={"skipped_case": "not meaningful for this dialect"},
            ),
        )
        register_sql_config(curated)
        register_sql_config(standard)

        # Each tier sees only its own member, so a hard-coded tier would return the wrong one.
        assert [
            type(config)
            for config in data_sources_for_tier_case(
                SupportTier.CANONICAL_EXPECTATIONS, "unrelated_case"
            )
        ] == [standard]
        assert [
            type(config)
            for config in data_sources_for_tier_case(SupportTier.CURATED_SQL, "unrelated_case")
        ] == [curated]

        # And the exclusion applies within the tier that declared it, not across tiers.
        assert data_sources_for_tier_case(SupportTier.CANONICAL_EXPECTATIONS, "skipped_case") == []
        assert [
            type(config)
            for config in data_sources_for_tier_case(SupportTier.CURATED_SQL, "skipped_case")
        ] == [curated]


class TestRegisterSqlConfigDuplicateLabel:
    def test_duplicate_label_raises_naming_both_classes(self) -> None:
        first = _make_config_class("First", _make_spec(label="dup-label", marker="first_marker"))
        second = _make_config_class("Second", _make_spec(label="dup-label", marker="second_marker"))
        register_sql_config(first)

        with pytest.raises(ValueError) as excinfo:
            register_sql_config(second)

        message = str(excinfo.value)
        assert "First" in message
        assert "Second" in message
        assert "dup-label" in message


class TestRegisterSqlConfigDuplicateMarker:
    def test_duplicate_marker_raises_naming_both_classes(self) -> None:
        first = _make_config_class("First", _make_spec(label="first-label", marker="dup_marker"))
        second = _make_config_class("Second", _make_spec(label="second-label", marker="dup_marker"))
        register_sql_config(first)

        with pytest.raises(ValueError) as excinfo:
            register_sql_config(second)

        message = str(excinfo.value)
        assert "First" in message
        assert "Second" in message
        assert "dup_marker" in message


class TestRegisterSqlConfigContainerProvisioning:
    def test_local_container_without_container_service_raises(self) -> None:
        config_class = _make_config_class(
            "NoService",
            _make_spec(
                provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
                container_service=None,
                tiers=frozenset({SupportTier.CURATED_SQL}),
            ),
        )

        with pytest.raises(ValueError) as excinfo:
            register_sql_config(config_class)

        message = str(excinfo.value)
        assert "NoService" in message
        # Naming the class is not enough: this record is malformed in exactly one way, but every
        # rejection in this module names the class that was rejected, so an assertion on the name
        # alone would pass for any of them — including one raised for a reason this test is not
        # about. The branch under test is `_validate_tier_claims`'s container obligation, which is
        # scaled to the tier claim; the neighbouring `_validate_container_provisioning` rejects the
        # opposite arrangement and says "without LOCAL_CONTAINER provisioning" instead. Asserting
        # the phrase that only the claim-scaled branch produces is what pins which one fired.
        assert "names no container_service" in message
        assert "claims tier membership" in message

    def test_container_service_without_local_container_raises(self) -> None:
        config_class = _make_config_class(
            "StrayService",
            _make_spec(
                provisioning=DataSourceProvisioning.LOCAL_FILE, container_service="throwaway"
            ),
        )

        with pytest.raises(ValueError) as excinfo:
            register_sql_config(config_class)

        assert "StrayService" in str(excinfo.value)


class TestRegisterSqlConfigEmptyFields:
    def test_empty_label_raises(self) -> None:
        config_class = _make_config_class("BlankLabel", _make_spec(label=""))

        with pytest.raises(ValueError, match="BlankLabel"):
            register_sql_config(config_class)

    def test_empty_marker_raises(self) -> None:
        config_class = _make_config_class("BlankMarker", _make_spec(marker=""))

        with pytest.raises(ValueError, match="BlankMarker"):
            register_sql_config(config_class)

    def test_empty_ci_lane_workflow_job_raises(self) -> None:
        config_class = _make_config_class(
            "BlankWorkflowJob",
            _make_spec(ci_lane=CiLaneRef(workflow_job="", marker_token="postgresql")),
        )

        with pytest.raises(ValueError, match="BlankWorkflowJob"):
            register_sql_config(config_class)

    def test_empty_ci_lane_marker_token_raises(self) -> None:
        config_class = _make_config_class(
            "BlankCiLane",
            _make_spec(ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="")),
        )

        with pytest.raises(ValueError, match="BlankCiLane"):
            register_sql_config(config_class)

    def test_non_positive_insert_parameter_limit_raises(self) -> None:
        config_class = _make_config_class("ZeroLimit", _make_spec(insert_parameter_limit=0))

        with pytest.raises(ValueError, match="ZeroLimit"):
            register_sql_config(config_class)

    def test_negative_insert_parameter_limit_raises(self) -> None:
        config_class = _make_config_class("NegativeLimit", _make_spec(insert_parameter_limit=-1))

        with pytest.raises(ValueError, match="NegativeLimit"):
            register_sql_config(config_class)


class TestRegisterSqlConfigTierCaseExclusionReasons:
    def test_empty_case_key_raises(self) -> None:
        config_class = _make_config_class(
            "BlankKey", _make_spec(tier_case_exclusions={"": "a reason"})
        )

        with pytest.raises(ValueError, match="BlankKey"):
            register_sql_config(config_class)

    def test_empty_reason_raises_naming_class_and_case_key(self) -> None:
        config_class = _make_config_class(
            "BlankReason", _make_spec(tier_case_exclusions={"some_case": ""})
        )

        with pytest.raises(ValueError) as excinfo:
            register_sql_config(config_class)

        message = str(excinfo.value)
        assert "BlankReason" in message
        assert "some_case" in message

    def test_whitespace_only_reason_raises_naming_class_and_case_key(self) -> None:
        config_class = _make_config_class(
            "WhitespaceReason", _make_spec(tier_case_exclusions={"some_case": "   "})
        )

        with pytest.raises(ValueError) as excinfo:
            register_sql_config(config_class)

        message = str(excinfo.value)
        assert "WhitespaceReason" in message
        assert "some_case" in message


class TestRegisterSqlConfigTierCaseExclusionCeiling:
    def test_exactly_two_exclusions_registers_cleanly(self) -> None:
        config_class = _make_config_class(
            "TwoExclusions",
            _make_spec(
                tier_case_exclusions={
                    "case_one": "dialect gap, see issue #1",
                    "case_two": "dialect gap, see issue #2",
                }
            ),
        )

        register_sql_config(config_class)

        assert config_class in iter_data_source_configs()

    def test_three_exclusions_raises_naming_class_count_and_all_keys(self) -> None:
        config_class = _make_config_class(
            "ThreeExclusions",
            _make_spec(
                tier_case_exclusions={
                    "case_one": "dialect gap, see issue #1",
                    "case_two": "dialect gap, see issue #2",
                    "case_three": "observed non-determinism, see issue #3",
                }
            ),
        )

        with pytest.raises(ValueError) as excinfo:
            register_sql_config(config_class)

        message = str(excinfo.value)
        assert "ThreeExclusions" in message
        assert "3" in message
        assert "case_one" in message
        assert "case_two" in message
        assert "case_three" in message


class TestRegisterSqlConfigTableSchemaItems:
    def test_non_callable_table_schema_items_raises(self) -> None:
        config_class = _make_config_class(
            "NotCallable",
            _make_spec(table_schema_items="not-a-callable"),
        )

        with pytest.raises(ValueError, match="NotCallable"):
            register_sql_config(config_class)

    def test_callable_table_schema_items_is_validated_without_being_invoked(self) -> None:
        calls: List[None] = []

        def factory() -> List[object]:
            calls.append(None)
            return []

        config_class = _make_config_class("Callable", _make_spec(table_schema_items=factory))

        register_sql_config(config_class)

        assert calls == []


class _HandWrittenControlConfig(DataSourceTestConfig):
    """A config written the way today's backends are, with `label` and `pytest_mark` as
    hand-coded properties. Used as the behavior-preservation control for
    `SqlDatasourceTestConfig`: the divergent label/marker pair (SQL Server's label is `mssql`,
    its marker is `sql_server`) is the case that most directly exercises whether a declaration-
    derived config produces the same identity as one written by hand.
    """

    @property
    @override
    def label(self) -> str:
        return "mssql"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.sql_server

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        raise NotImplementedError("not exercised by these tests")


_THROWAWAY_DECLARED_SPEC = SqlBackendSpec(
    label="mssql",
    public_name="SQL Server",
    marker="sql_server",
    provisioning=DataSourceProvisioning.LOCAL_FILE,
    ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="sql_server"),
    uses_schema=True,
)


class _DeclaredConfig(SqlDatasourceTestConfig):
    """Throwaway config that derives its identity from a declared `SqlBackendSpec`, mirroring
    `_HandWrittenControlConfig`'s label and marker exactly."""

    DATA_SOURCE_SPEC = _THROWAWAY_DECLARED_SPEC

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        raise NotImplementedError("not exercised by these tests")


class TestSqlDatasourceTestConfigDerivesIdentity:
    def test_derives_label_and_mark_matching_a_hand_written_control(self) -> None:
        control = _HandWrittenControlConfig()
        declared = _DeclaredConfig()

        assert declared.label == control.label == "mssql"
        assert declared.pytest_mark == control.pytest_mark == pytest.mark.sql_server
        assert declared.test_id == control.test_id
        assert hash(declared) == hash(control)
        assert declared == control


class TestSqlDatasourceTestConfigOverrideSeam:
    def test_instance_level_backend_spec_overrides_the_class_declaration(self) -> None:
        override_spec = SqlBackendSpec(
            label="ad-hoc",
            public_name="Ad-hoc SQL",
            marker="generic_sql",
            provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="generic_sql"),
            uses_schema=False,
        )

        default_instance = _DeclaredConfig()
        overridden_instance = _DeclaredConfig(backend_spec_override=override_spec)

        assert default_instance.label == "mssql"
        assert default_instance.pytest_mark == pytest.mark.sql_server
        assert overridden_instance.label == "ad-hoc"
        assert overridden_instance.pytest_mark == pytest.mark.generic_sql
        # The class-level declaration itself is untouched by the per-instance override.
        assert _DeclaredConfig.DATA_SOURCE_SPEC is _THROWAWAY_DECLARED_SPEC


class TestSqlDatasourceTestConfigSatisfiesRegistrationProtocol:
    def test_a_declared_config_class_registers_successfully(self) -> None:
        # `register_sql_config` is typed to accept only a class exposing
        # `DATA_SOURCE_SPEC: ClassVar[SqlBackendSpec]`. This call site is the first proof, under
        # mypy, that a real config class built on the declaration-derived base satisfies that
        # shape structurally rather than by explicit inheritance.
        with isolated_registry():
            register_sql_config(_DeclaredConfig)

            assert _DeclaredConfig in iter_data_source_configs()


_THROWAWAY_CORE_SPEC = DataSourceSpec(
    label="mssql",
    public_name="SQL Server",
    marker="sql_server",
    provisioning=DataSourceProvisioning.LOCAL_FILE,
)


class _CoreDeclaredConfig(DataSourceTestConfig):
    """Throwaway config that derives its identity from a plain core record rather than from a SQL
    sub-record, mirroring `_HandWrittenControlConfig`'s label and marker exactly.

    Deliberately not a SQL config: the declaration slot and the identity derived from it live on
    the shared config base, so a data source with no dialect facts at all states its identity the
    same way a SQL backend does.
    """

    DATA_SOURCE_SPEC = _THROWAWAY_CORE_SPEC

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        raise NotImplementedError("not exercised by these tests")


class TestSharedConfigBaseDerivesIdentityFromACoreRecord:
    """Each claim is its own test so that a defect breaking one of them is reported as that one.

    Bundled into a single method, the first failing assertion would hide every claim after it,
    and a derived claim that is never evaluated cannot be told apart from one that holds.
    """

    def test_derives_the_label_of_a_hand_written_control(self) -> None:
        assert _CoreDeclaredConfig().label == _HandWrittenControlConfig().label == "mssql"

    def test_derives_the_mark_of_a_hand_written_control(self) -> None:
        declared_mark = _CoreDeclaredConfig().pytest_mark
        assert declared_mark == _HandWrittenControlConfig().pytest_mark == pytest.mark.sql_server

    def test_reports_the_same_test_id_as_a_hand_written_control(self) -> None:
        assert _CoreDeclaredConfig().test_id == _HandWrittenControlConfig().test_id

    def test_hashes_to_the_same_value_as_a_hand_written_control(self) -> None:
        assert hash(_CoreDeclaredConfig()) == hash(_HandWrittenControlConfig())

    def test_compares_equal_to_a_hand_written_control(self) -> None:
        assert _CoreDeclaredConfig() == _HandWrittenControlConfig()

    def test_the_declaration_takes_no_part_in_construction_equality_or_hashing(self) -> None:
        """The slot is a class attribute, not a dataclass field.

        The dataclass machinery recognizes it as one only by resolving the name `ClassVar` in the
        defining module's namespace at runtime. Were that name unavailable there - imported for
        type checking alone, say - the annotation would read as an ordinary field instead, and
        the declaration would silently join every config's constructor, equality and hash. That
        failure is invisible at the declaration site, so it is checked here.
        """
        assert "DATA_SOURCE_SPEC" not in {f.name for f in fields(DataSourceTestConfig)}
        assert "DATA_SOURCE_SPEC" not in {f.name for f in fields(_CoreDeclaredConfig)}
        assert "DATA_SOURCE_SPEC" not in {f.name for f in fields(_DeclaredConfig)}


class TestTheDeclarationSlotIsDeclaredExactlyOnce:
    def test_only_the_shared_config_base_annotates_the_declaration(self) -> None:
        """Two declaration slots must never coexist.

        A subclass that re-annotates an inherited class variable with a narrower type is not
        narrowing it - class variables are invariant - it is a second declaration of the same
        fact, and a reader then has two places to look for it. The type checker does not object
        to one, which is why this test exists rather than a type annotation being relied on to
        catch it. Every narrowing therefore happens in an accessor, guarded by an invariant
        registration enforces, and the slot itself is annotated once.

        The two registration protocols are expected alongside the base: each states a shape a
        config class must have in order to be enrolled, which is a requirement placed on a
        declaration rather than a second place to make one. `_DeclaresDataSourceSpec` states the
        core-width shape the registry stores and every accessor hands back;
        `_DeclaresSqlBackendSpec` narrows it for the SQL-specific entry point, and that narrowing
        exists because a protocol variable member is invariant, so a config whose own symbol is
        inferred at the sub-record type would otherwise stop satisfying a core-width protocol at
        its registration site.
        """
        package_name = DataSourceTestConfig.__module__.rsplit(".", 1)[0]
        importlib.import_module(package_name)

        annotating: set[str] = set()
        for module_name, module in list(sys.modules.items()):
            if module_name != package_name and not module_name.startswith(f"{package_name}."):
                continue
            for obj in vars(module).values():
                if not isinstance(obj, type) or not obj.__module__.startswith(package_name):
                    continue
                if "DATA_SOURCE_SPEC" in vars(obj).get("__annotations__", {}):
                    annotating.add(obj.__qualname__)

        assert annotating == {
            "DataSourceTestConfig",
            "_DeclaresSqlBackendSpec",
            "_DeclaresDataSourceSpec",
        }


class TestLocallyVerifiableBackendsRegisterInLabelOrder:
    def test_postgres_mysql_sql_server_and_sqlite_appear_in_label_order(self) -> None:
        # Re-register the four real, locally verifiable backend configs inside this module's
        # isolation seam. Each class is already enrolled once, for real, at import time via its
        # own `@register_sql_config` decorator; re-registering it here (against the seam's
        # cleared, isolated dicts, not the real registry) proves the same fact the real import
        # already established, without depending on import order relative to the eight other
        # backend modules that will be added in later tasks.
        from tests.integration.test_utils.data_source_config.mysql import (
            MySQLDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.postgres import (
            PostgreSQLDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.sql_server import (
            SQLServerDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.sqlite import (
            SqliteDatasourceTestConfig,
        )

        for config_class in (
            PostgreSQLDatasourceTestConfig,
            MySQLDatasourceTestConfig,
            SQLServerDatasourceTestConfig,
            SqliteDatasourceTestConfig,
        ):
            register_sql_config(config_class)

        # This is the seam-local ordering subset, not the registered-set pin further down this
        # module. The two literals look alike - same classes, same trailing label comments - but
        # they answer different questions, and only the other one has to grow when a backend is
        # added. Edit by line, not by matching on these lines.
        assert iter_data_source_configs() == (
            SQLServerDatasourceTestConfig,  # mssql
            MySQLDatasourceTestConfig,  # mysql
            PostgreSQLDatasourceTestConfig,  # postgresql
            SqliteDatasourceTestConfig,  # sqlite
        )


class TestCredentialGatedBackendsRegisterInLabelOrder:
    def test_bigquery_databricks_redshift_and_snowflake_appear_in_label_order(self) -> None:
        # These four cannot have their suites run locally (no credentials for any of the four
        # hosted services), but registration itself has no dependency on credentials or on the
        # dialect package being installed - it only reads the declared spec. Re-registering the
        # real classes here, against this module's isolated seam, proves label ordering without
        # depending on import order relative to the other backend modules.
        from tests.integration.test_utils.data_source_config.big_query import (
            BigQueryDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.databricks import (
            DatabricksDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.redshift import (
            RedshiftDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.snowflake import (
            SnowflakeDatasourceTestConfig,
        )

        # Registered deliberately out of label order: an implementation returning insertion
        # order would produce this tuple instead of the sorted one asserted below, so the
        # assertion discriminates label ordering rather than merely recording these labels.
        for config_class in (
            SnowflakeDatasourceTestConfig,
            BigQueryDatasourceTestConfig,
            RedshiftDatasourceTestConfig,
            DatabricksDatasourceTestConfig,
        ):
            register_sql_config(config_class)

        assert iter_data_source_configs() == (
            BigQueryDatasourceTestConfig,  # big-query
            DatabricksDatasourceTestConfig,  # databricks
            RedshiftDatasourceTestConfig,  # redshift
            SnowflakeDatasourceTestConfig,  # snowflake
        )


class TestGenericSqlEscapeHatchIsDeclaredButUnregistered:
    """The ad-hoc, caller-supplied-connection-string config has no fixed identity to enrol, so
    unlike the eight dialect-specific backends it must never appear in the registry that gates
    CI - even though, like them, it now derives its identity from a declared spec.
    """

    def test_is_absent_from_the_registry(self) -> None:
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        assert GenericSQLDatasourceTestConfig not in iter_data_source_configs()
        assert "generic_sql" not in {
            backend.DATA_SOURCE_SPEC.label for backend in iter_data_source_configs()
        }

    def test_its_declared_record_still_passes_registration_validation(self) -> None:
        """Proves the absence above is a deliberate omission, not a side effect of the record
        failing registration's own validation.

        This registers the *record*, not the config class, and that distinction is the whole
        content of the test now. The record is well-formed and enrols cleanly. What it does not
        do — and deliberately cannot — is declare the shared canonical expectation
        parameterization criterion, because it has no fixed identity to declare anything with; so
        registering it *with* its config class is now rejected, which the next test pins. Neither
        outcome reaches the real registry, because the escape hatch carries no registration
        decorator at all.
        """
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        with isolated_registry():
            register_data_source(GenericSQLDatasourceTestConfig.DATA_SOURCE_SPEC)

            assert [spec.label for spec in iter_data_source_specs()] == ["generic_sql"]

    def test_registering_it_with_its_config_class_is_rejected_for_the_criterion(self) -> None:
        """The one way the escape hatch could reach the mandatory-declaration rule, and the
        reason it never does.

        It names an execution engine and declares no criterion, and it is deliberately absent
        from the non-participants literal — an exemption there would be an exemption for a
        registration that never happens. The rule is unreachable for it in the real registry
        because `generic_sql.py` carries no registration decorator; this test is the only place
        the registration is ever attempted.
        """
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        with isolated_registry(), pytest.raises(ValueError) as excinfo:
            register_sql_config(GenericSQLDatasourceTestConfig)

        assert "does not declare the shared canonical expectation parameterization" in str(
            excinfo.value
        )


class TestGenericSqlEscapeHatchAutocommitSeam:
    def test_default_instance_reports_explicit_commit(self) -> None:
        from tests.integration.test_utils.data_source_config.backend_spec import TransactionMode
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        config = GenericSQLDatasourceTestConfig()

        assert config.backend_spec.transaction_mode == TransactionMode.EXPLICIT_COMMIT

    def test_autocommit_instance_reports_autocommit_mode(self) -> None:
        from tests.integration.test_utils.data_source_config.backend_spec import TransactionMode
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        config = GenericSQLDatasourceTestConfig(autocommit=True)

        assert config.backend_spec.transaction_mode == TransactionMode.AUTOCOMMIT

    def test_autocommit_does_not_mutate_the_class_level_declaration(self) -> None:
        from tests.integration.test_utils.data_source_config.backend_spec import TransactionMode
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        GenericSQLDatasourceTestConfig(autocommit=True)

        assert (
            GenericSQLDatasourceTestConfig.DATA_SOURCE_SPEC.transaction_mode
            == TransactionMode.EXPLICIT_COMMIT
        )

    def test_autocommit_varies_the_label_so_two_configs_no_longer_collide(self) -> None:
        """An instance whose only observable difference from another is its transaction mode
        must not compare equal to it and must not share a cache key with it: the session-scoped
        batch-setup cache is a plain `dict` keyed on config equality, and equality here is
        defined in terms of `label`, so two configs sharing a label but disagreeing on
        transaction mode would collide - the second one silently reusing the first one's cached
        setup and inheriting its commit behavior.
        """
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        explicit = GenericSQLDatasourceTestConfig(
            connection_string="mysql+pymysql://x/y", autocommit=False
        )
        autocommit = GenericSQLDatasourceTestConfig(
            connection_string="mysql+pymysql://x/y", autocommit=True
        )

        assert explicit.label != autocommit.label
        assert explicit != autocommit
        assert hash(explicit) != hash(autocommit)

        # The exact shape the session-scoped cache uses: a plain dict keyed on the config.
        cache = {explicit: "setup-for-explicit"}
        assert autocommit not in cache


class TestGenericSqlEscapeHatchEnvironmentAutocommit:
    """`GX_TEST_GENERIC_SQL_AUTOCOMMIT` is an out-of-code way to ask for the same behavior the
    `autocommit` field declares in code. It is read once a batch setup is constructed for a
    config, not when the config itself is constructed or when the harness is imported - so a
    config built before the variable is set still picks it up once a batch setup is built for
    it.
    """

    def test_env_var_is_read_at_batch_setup_construction_not_at_config_construction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from typing import cast

        import pandas as pd

        from tests.integration.test_utils.data_source_config.backend_spec import TransactionMode
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLBatchTestSetup,
            GenericSQLDatasourceTestConfig,
        )

        monkeypatch.setenv("GX_TEST_GENERIC_SQL_AUTOCOMMIT", "1")

        config = GenericSQLDatasourceTestConfig(connection_string="sqlite:///:memory:")
        # The config's own field is untouched by the environment, so its declaration - read on
        # its own, with no batch setup involved - still reports the explicit-commit default.
        assert config.autocommit is False
        assert config.backend_spec.transaction_mode == TransactionMode.EXPLICIT_COMMIT

        batch_setup = GenericSQLBatchTestSetup(
            config=config,
            data=pd.DataFrame({"a": [1]}),
            extra_data={},
            context=cast("AbstractDataContext", object()),
        )

        # Once a batch setup exists for that same config, the environment variable it read at
        # construction takes effect.
        assert batch_setup.backend_spec.transaction_mode == TransactionMode.AUTOCOMMIT

    def test_env_var_unset_leaves_explicit_commit_in_place(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Clear the variable rather than assuming it is unset: exporting it is the escape
        # hatch's own documented usage, so a developer running that way would otherwise turn
        # this negative control red for a reason that has nothing to do with what it asserts.
        monkeypatch.delenv("GX_TEST_GENERIC_SQL_AUTOCOMMIT", raising=False)
        from typing import cast

        import pandas as pd

        from tests.integration.test_utils.data_source_config.backend_spec import TransactionMode
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLBatchTestSetup,
            GenericSQLDatasourceTestConfig,
        )

        config = GenericSQLDatasourceTestConfig(connection_string="sqlite:///:memory:")
        batch_setup = GenericSQLBatchTestSetup(
            config=config,
            data=pd.DataFrame({"a": [1]}),
            extra_data={},
            context=cast("AbstractDataContext", object()),
        )

        assert batch_setup.backend_spec.transaction_mode == TransactionMode.EXPLICIT_COMMIT


class TestGenericSqlEscapeHatchHashRegression:
    """Regression coverage for a trap in re-decorating a `SqlDatasourceTestConfig` subclass with
    a bare `@dataclass(frozen=True)`: doing so silently regenerates `__eq__`/`__hash__`,
    discarding the hand-written `__hash__` that reduces `extra_column_types` to a hashable tuple
    before hashing it. The generated replacement hashes the raw `dict` value instead, which
    raises unconditionally - even for a config with no `extra_column_types` at all, since the
    empty-dict default is still a `dict`. This has no test-suite-visible symptom (nothing calls
    `hash()` on a config directly in the ordinary suite), which is exactly why it must be pinned
    here rather than left to be noticed by whatever else happens to depend on it.
    """

    def test_hash_does_not_raise_on_a_default_instance(self) -> None:
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        hash(GenericSQLDatasourceTestConfig())  # must not raise TypeError: unhashable type: dict

    def test_hash_does_not_raise_with_non_empty_extra_column_types(self) -> None:
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        config = GenericSQLDatasourceTestConfig(extra_column_types={"other_table": {"col": str}})

        hash(config)  # must not raise TypeError: unhashable type: dict

    def test_eq_and_hash_resolve_to_the_shared_base_implementation(self) -> None:
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLDatasourceTestConfig,
        )

        assert (
            GenericSQLDatasourceTestConfig.__eq__.__qualname__
            == DataSourceTestConfig.__eq__.__qualname__
        )
        assert (
            GenericSQLDatasourceTestConfig.__hash__.__qualname__
            == DataSourceTestConfig.__hash__.__qualname__
        )


class TestGenericSqlEscapeHatchPublicNames:
    """The escape hatch is imported by name from several call sites across the suite; this pins
    that both names used elsewhere in the repo still resolve after the base class changes.
    """

    def test_config_and_batch_setup_classes_are_still_importable_and_constructible(self) -> None:
        from tests.integration.test_utils.data_source_config.generic_sql import (
            GenericSQLBatchTestSetup,
            GenericSQLDatasourceTestConfig,
        )

        config = GenericSQLDatasourceTestConfig(connection_string="sqlite:///:memory:")

        assert config.label == "generic_sql"
        assert config.pytest_mark == pytest.mark.generic_sql
        assert GenericSQLBatchTestSetup is not None


class TestRegisteredBackendsKeepTheInheritedHash:
    def test_no_registered_backend_regenerated_eq_or_hash(self) -> None:
        """Every registered config must inherit `DataSourceTestConfig`'s `__eq__`/`__hash__`.

        Those are hand-written to reduce `extra_column_types` to a hashable tuple first. A config
        re-decorated with a bare `@dataclass(frozen=True)` silently regenerates both, and the
        generated `__hash__` hashes that raw `dict` and raises `TypeError` on every instance.

        This is checked here rather than in `__init_subclass__` because a class decorator runs
        *after* class creation: `__init_subclass__` observes the class before `@dataclass` has
        replaced anything, so it cannot see the regeneration it would be trying to prevent.

        The failure this guards against costs session time rather than turning a test red — a
        config that cannot be hashed, or that hashes inconsistently, degrades the batch-setup
        cache instead of failing — which is why it needs a mechanical check rather than a
        convention.
        """

        # Walks subclasses rather than the registry: this module's autouse fixture wraps every
        # test in the isolation seam, which clears the registry, so iterating it here would
        # examine nothing and pass vacuously. Subclasses also reach configs that are declared
        # but deliberately unregistered.
        def _descendants(cls: type) -> Iterator[type]:
            for sub in cls.__subclasses__():
                yield sub
                yield from _descendants(sub)

        checked = [
            c for c in _descendants(SqlDatasourceTestConfig) if not c.__name__.startswith("_")
        ]
        assert checked, "no concrete SQL config subclasses were found to check"

        for config_class in checked:
            assert config_class.__eq__ is DataSourceTestConfig.__eq__, (
                f"{config_class.__name__} regenerated __eq__; a subclass re-decorated with "
                f"@dataclass must pass eq=False"
            )
            assert config_class.__hash__ is DataSourceTestConfig.__hash__, (
                f"{config_class.__name__} regenerated __hash__; a subclass re-decorated with "
                f"@dataclass must pass eq=False"
            )
            hash(config_class())  # a regenerated hash raises TypeError here


class TestStandardDataSourceListsMatchDeclaredMembership:
    """Regression pin for the four standard data-source lists now defined once in `tiers.py`.

    Every literal below is written out here rather than derived from the module under test — so a
    mistake in the derivation shows up as a mismatch here rather than agreeing with itself. The
    pandas and Spark literals are transcribed from the two metrics conftest modules exactly as
    they existed before those lists gained a single shared definition, except for
    `PANDAS_DATA_SOURCES`'s order. That list was hand-written non-alphabetically — the filesystem
    CSV config ahead of the DataFrame config — and that order was preserved on purpose for as long
    as the list was a literal. It is now **label order**, DataFrame ahead of filesystem CSV,
    because the list is derived from the registry and the registry orders every accessor by record
    label. Membership is unchanged; only the order moved, and it moved because the hand-written
    literal it replaced is gone, not because anything about either data source changed. Nothing
    reads either list positionally — both are pytest parameterization sources — so the visible
    consequence is the order test ids are generated in.

    The two SQL literals began as the same transcription and have since widened, once, by
    declaration. MySQL, Microsoft SQL Server and Redshift appeared in the hand-written lists that
    `test_canonical_expectations.py` held until it was retired but declared no tier, so the derived
    lists — the pair the metrics tree consumes — omitted all three, and roughly 165 metrics
    parameterizations never ran against them while the same backends ran the full expectation
    suite. Declaring `SupportTier.CANONICAL_EXPECTATIONS` on those three configs is what puts
    them in the literals below. What now stops that kind of divergence recurring is the mandatory
    shared-parameterization criterion, which makes a config joining the shared parameterization
    without declaring it a registration-time error rather than a silent omission, together with
    `TestRelocatedDataSourceListsMatchTheirCapturedMembership` below, which pins each relocated
    list against the membership captured from the retired module before anything moved.

    The four constants imported above are captured at module-import time, before any test in this
    module runs. That matters because this module's `_snapshot_registry` fixture clears the
    registry around every test: asserting against those already-built module-level objects, or
    reading `PANDAS_DATA_SOURCES` and `SPARK_DATA_SOURCES` (which never touch the registry) is
    safe, while re-deriving `SQL_DATA_SOURCES` from the registry *inside* a test body would
    observe the isolation seam's emptied registry and pass vacuously.
    """

    def test_pandas_data_sources_match_declared_membership_and_order(self) -> None:
        assert [
            PandasDataFrameDatasourceTestConfig(),
            PandasFilesystemCsvDatasourceTestConfig(),
        ] == PANDAS_DATA_SOURCES

    def test_spark_data_sources_match_declared_membership_and_order(self) -> None:
        assert [
            SparkFilesystemCsvDatasourceTestConfig(),
        ] == SPARK_DATA_SOURCES

    def test_sql_data_sources_match_declared_membership_and_order(self) -> None:
        assert [
            BigQueryDatasourceTestConfig(),
            DatabricksDatasourceTestConfig(),
            SQLServerDatasourceTestConfig(),
            MySQLDatasourceTestConfig(),
            PostgreSQLDatasourceTestConfig(),
            RedshiftDatasourceTestConfig(),
            SnowflakeDatasourceTestConfig(),
            SqliteDatasourceTestConfig(),
        ] == SQL_DATA_SOURCES

    def test_all_data_sources_match_declared_membership_and_order(self) -> None:
        """The eleven the shared parameterization runs against, in label order.

        Membership is exactly what it was when this list was `PANDAS + SPARK + SQL`; only the
        order moved, because the list is now a single read of the shared-parameterization tier
        rather than three derivations concatenated, and every registry accessor orders by record
        label. The three non-SQL entries therefore sit among the SQL ones rather than ahead of
        them. Nothing reads this list positionally — it is a pytest parameterization source — so
        the visible consequence is the order test ids are generated in.
        """
        assert [
            BigQueryDatasourceTestConfig(),
            DatabricksDatasourceTestConfig(),
            SQLServerDatasourceTestConfig(),
            MySQLDatasourceTestConfig(),
            PandasDataFrameDatasourceTestConfig(),
            PandasFilesystemCsvDatasourceTestConfig(),
            PostgreSQLDatasourceTestConfig(),
            RedshiftDatasourceTestConfig(),
            SnowflakeDatasourceTestConfig(),
            SparkFilesystemCsvDatasourceTestConfig(),
            SqliteDatasourceTestConfig(),
        ] == ALL_DATA_SOURCES


class TestRelocatedDataSourceListsMatchTheirCapturedMembership:
    """The pin that replaces the derived-vs-hand-written equality, now that only one list is left.

    `SQL_DATA_SOURCES` and `ALL_DATA_SOURCES` used to be defined twice under the same name — derived
    from tier declarations in `tiers.py`, and written by hand in the retired
    `test_canonical_expectations.py` — and the class that stood here compared the two. Three
    backends had already drifted between them: MySQL, Microsoft SQL Server and Redshift ran every
    expectation module through the hand-written lists while declaring no tier, so they were absent
    from every list-driven metrics parameterization with no error, no skip and no warning.

    That comparison had two terms only while the duplicate existed. The duplicate is now deleted and
    its consumers import the derived pair, so the equality has nothing left to compare and asserting
    it would be vacuous. **What carries its invariant forward is two other things, both of which
    exist independently of this class**: the mandatory shared-parameterization criterion, which
    makes a config joining the parameterization without declaring it a registration-time error
    rather than a silent divergence, and the membership pins below, which fix every relocated list
    against the membership *captured from the retired module before anything moved* — not against
    the code that now produces it, which would agree with itself.

    The literals below are that capture, transcribed by label. `generic_sql` is spelled with an
    underscore where every other label is hyphenated; that is the tree's own inconsistency and the
    assertions match it literally.
    """

    RECORDED_ALL_DATA_SOURCES: ClassVar[FrozenSet[str]] = frozenset(
        {
            "big-query",
            "databricks",
            "mssql",
            "mysql",
            "pandas-data-frame",
            "pandas-filesystem-csv",
            "postgresql",
            "redshift",
            "generic_sql",
            "snowflake",
            "spark-filesystem-csv",
            "sqlite",
        }
    )
    """The twelve labels the retired module's `ALL_DATA_SOURCES` held, captured before the move."""

    RECORDED_SQL_DATA_SOURCES: ClassVar[FrozenSet[str]] = frozenset(
        {
            "big-query",
            "databricks",
            "mssql",
            "mysql",
            "postgresql",
            "redshift",
            "generic_sql",
            "snowflake",
            "sqlite",
        }
    )
    """The nine labels the retired module's `SQL_DATA_SOURCES` held, captured before the move."""

    RECORDED_NON_SQL_DATA_SOURCES: ClassVar[FrozenSet[str]] = frozenset(
        {"pandas-data-frame", "pandas-filesystem-csv", "spark-filesystem-csv"}
    )

    RECORDED_DATE_COMPARISON_DATA_SOURCES: ClassVar[FrozenSet[str]] = frozenset(
        {
            "big-query",
            "databricks",
            "mssql",
            "mysql",
            "pandas-data-frame",
            "postgresql",
            "redshift",
            "generic_sql",
            "snowflake",
            "spark-filesystem-csv",
        }
    )

    RECORDED_JUST_PANDAS_DATA_SOURCES: ClassVar[FrozenSet[str]] = frozenset({"pandas-data-frame"})

    ESCAPE_HATCH_LABEL: ClassVar[str] = "generic_sql"
    """The one entry that left the two shared lists, named as the literal the capture records.

    It is written out rather than read from `GenericSQLDatasourceTestConfig`, so that renaming the
    config's label fails this assertion instead of silently redefining what left.
    """

    @staticmethod
    def _labels(configs: Sequence[DataSourceTestConfig]) -> FrozenSet[str]:
        return frozenset(type(config).DATA_SOURCE_SPEC.label for config in configs)

    def test_all_data_sources_lost_exactly_the_escape_hatch(self) -> None:
        """Asserted as a set difference in both directions, never as a new count.

        Twelve to eleven by exactly one entry. Pinning `len(...) == 11` instead would pass just as
        happily if a second backend vanished and a third appeared, which is the failure this
        assertion exists to catch.
        """
        derived = self._labels(ALL_DATA_SOURCES)

        assert self.RECORDED_ALL_DATA_SOURCES - derived == {self.ESCAPE_HATCH_LABEL}
        assert derived - self.RECORDED_ALL_DATA_SOURCES == frozenset()

    def test_sql_data_sources_lost_exactly_the_escape_hatch(self) -> None:
        """Nine to eight by exactly the same one entry, asserted the same way."""
        derived = self._labels(SQL_DATA_SOURCES)

        assert self.RECORDED_SQL_DATA_SOURCES - derived == {self.ESCAPE_HATCH_LABEL}
        assert derived - self.RECORDED_SQL_DATA_SOURCES == frozenset()

    def test_non_sql_data_sources_match_the_captured_membership(self) -> None:
        """The one relocated list that is derived — from the execution engine, which reproduces it.

        The assertion is against the captured labels, not against
        `PANDAS_DATA_SOURCES + SPARK_DATA_SOURCES`, which is the expression that now builds it.
        """
        assert self._labels(NON_SQL_DATA_SOURCES) == self.RECORDED_NON_SQL_DATA_SOURCES

    def test_date_comparison_data_sources_match_the_captured_membership(self) -> None:
        """Relocated and still declared, escape hatch included.

        The entry left the two *derived* lists because those are the set that gates CI. This list is
        neither derived nor CI-gating, so it keeps all ten it was captured with.
        """
        assert (
            self._labels(DATA_SOURCES_THAT_SUPPORT_DATE_COMPARISONS)
            == self.RECORDED_DATE_COMPARISON_DATA_SOURCES
        )

    def test_just_pandas_data_sources_match_the_captured_membership(self) -> None:
        assert self._labels(JUST_PANDAS_DATA_SOURCES) == self.RECORDED_JUST_PANDAS_DATA_SOURCES

    def test_no_module_still_exports_the_retired_duplicate_lists(self) -> None:
        """The collision is gone because the module is gone, and this says so by looking.

        A rename would have left two differently-populated lists under two names and removed only
        the invitation to compare them. Deleting it is what removed the divergence, and a module
        reappearing under that path — with either list in it — reopens the divergence the pins
        above cannot see,
        because they only ever read the derived pair.
        """
        retired = (
            Path(__file__).parent
            / "integration"
            / "data_sources_and_expectations"
            / "test_canonical_expectations.py"
        )

        assert not retired.exists(), f"the retired module reappeared at {retired}"


class TestCuratedSqlDataSourcesEqualsClickHouseOracleSingleStoreAndTrino:
    """Regression pin for the curated tier's members, in label order.

    `CURATED_SQL_DATA_SOURCES` was empty until a backend declared curated-tier membership, so
    every assertion involving it was vacuously true (empty equals empty). SingleStore was the
    first backend to join that tier, Trino was the second, ClickHouse was the third, and Oracle
    is the fourth — all four are what make this pin non-vacuous: it fails on a curated-tier
    backend registering without also joining this literal, and fails just as loudly on the
    reverse — a config landing in this literal without the corresponding registration.

    `CURATED_SQL_DATA_SOURCES` is imported at this module's own import time (see the module
    docstring on `TestStandardDataSourceListsMatchPreChangeMembership` above for why that
    matters): it is safe to assert against directly here, unlike a call-time re-derivation from
    the registry, which this module's `_snapshot_registry` autouse fixture would observe as
    empty.
    """

    def test_curated_sql_data_sources_equals_clickhouse_oracle_singlestore_and_trino_in_label_order(
        self,
    ) -> None:
        from tests.integration.test_utils.data_source_config.clickhouse import (
            ClickHouseDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.oracle import (
            OracleDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.trino import (
            TrinoDatasourceTestConfig,
        )

        assert [
            ClickHouseDatasourceTestConfig(),
            OracleDatasourceTestConfig(),
            SingleStoreDatasourceTestConfig(),
            TrinoDatasourceTestConfig(),
        ] == CURATED_SQL_DATA_SOURCES


class TestMetricsConftestsReexportTheSharedDefinition:
    """Both metrics conftest modules must expose the exact same list objects `tiers.py` builds,
    not equal-but-separate copies: an equal-but-separate list would let the two drift again the
    next time someone edits one and not the other, which is the failure mode this whole change
    removes.
    """

    def test_both_conftests_expose_objects_identical_to_the_shared_definition(self) -> None:
        import tests.integration.metrics.conftest as integration_metrics_conftest
        import tests.metrics.conftest as metrics_conftest

        assert metrics_conftest.PANDAS_DATA_SOURCES is PANDAS_DATA_SOURCES
        assert metrics_conftest.SPARK_DATA_SOURCES is SPARK_DATA_SOURCES
        assert metrics_conftest.SQL_DATA_SOURCES is SQL_DATA_SOURCES
        assert metrics_conftest.ALL_DATA_SOURCES is ALL_DATA_SOURCES

        assert integration_metrics_conftest.PANDAS_DATA_SOURCES is PANDAS_DATA_SOURCES
        assert integration_metrics_conftest.SPARK_DATA_SOURCES is SPARK_DATA_SOURCES
        assert integration_metrics_conftest.SQL_DATA_SOURCES is SQL_DATA_SOURCES
        assert integration_metrics_conftest.ALL_DATA_SOURCES is ALL_DATA_SOURCES


# Captured at this module's own import time — after the `tests.integration.test_utils.
# data_source_config` import above has already run that package's `__init__`, which imports every
# backend module and only then imports `tiers`, and before any test in this module runs (in
# particular, before this module's own `_snapshot_registry` autouse fixture ever clears the
# registry). `tiers.py` builds `SQL_DATA_SOURCES` and `CURATED_SQL_DATA_SOURCES` once, at *its*
# import time, from whatever the registry holds at that moment. If some backend module were imported
# after `tiers` instead of before it, that backend would finish registering only once this module's
# own top-level import statement reaches it — later than `tiers` already built its lists — so it
# would be absent from both lists even though `data_source_configs_for_tier` called here, afterward,
# reports it correctly. Comparing the two below is what turns that ordering accident into a failing
# test instead of a silent gap: the repo's own import-sorter routinely places a new backend module's
# import after `tiers`'s for any module name that sorts alphabetically later, and nothing else in
# this suite would catch the result.
# `data_source_configs_for_tier` walks every config-bound registration, not only the SQL ones, so
# this tuple is the shared-parameterization tier's whole membership — the eight SQL backends and the
# three non-SQL configs that now declare the same criterion. `ALL_DATA_SOURCES` is pinned against
# it directly; `SQL_DATA_SOURCES` against its SQL-engine subset.
_REGISTERED_CANONICAL_EXPECTATIONS = tuple(
    data_source_configs_for_tier(SupportTier.CANONICAL_EXPECTATIONS)
)
_REGISTERED_CURATED_SQL = tuple(data_source_configs_for_tier(SupportTier.CURATED_SQL))

# The same capture, for the same reason, over the two engine-keyed lists. `tiers.py` builds
# `PANDAS_DATA_SOURCES` and `SPARK_DATA_SOURCES` once at its own import time, exactly as it builds
# the two tier-keyed lists, so a config module imported after `tiers` is silently absent from the
# built list while `data_source_configs_for_engine`, called here afterwards, reports it correctly.
# Nothing compared the two for the engine-keyed pair until now.
_REGISTERED_PANDAS_CONFIGS = tuple(data_source_configs_for_engine(ExecutionEngineKind.PANDAS))
_REGISTERED_SPARK_CONFIGS = tuple(data_source_configs_for_engine(ExecutionEngineKind.SPARK))

# Also captured here, at this module's own import time and for the same reason as the two tuples
# above: this module's `_snapshot_registry` autouse fixture clears the registry around every test,
# so a test body calling `iter_data_source_configs()` directly would iterate nothing and pass
# vacuously. Unlike the two tuples above, this one is not sensitive to import order relative to
# `tiers.py` - it is the whole registered set, not a tier-filtered derivation of it - but it
# still has to be
# read before any test runs, hence the same module-scope placement.
_REGISTERED_CONFIGS: Tuple[type, ...] = tuple(iter_data_source_configs())


class TestRegisteredConfigsEqualTheFifteenInLabelOrder:
    """Pins the registry itself: every registered config class, named individually, in label order.

    Not every entry is a SQL backend: a config the harness drives registers here whether or not its
    declaration carries dialect facts, which is why the set this pins is "every registered config"
    rather than "every registered SQL backend".

    This is an *equality* assertion against an *ordered* literal naming every registered class -
    not a subset check, not a membership check, not a count. That shape is what makes registering
    a sixteenth config without extending this literal fail immediately: "register the config" and
    "extend this literal" become one change with a single, same-change failure signal, rather than
    a widening nobody notices until something downstream quietly starts seeing one more backend
    than it expected. A subset or count check would let a new registration pass silently here,
    which defeats the point, so neither is an acceptable substitute for the other.

    This module runs in a lane that installs no SQL dialect driver at all, and importing this
    module imports the whole harness package first, which in turn imports every backend module -
    each one registering itself as a side effect of being imported. An equality assertion over all
    twelve registered classes therefore runs only in a process where every backend module imported
    successfully with every dialect driver absent.

    Be precise about which half of that each mechanism carries. A backend module that fails to
    import takes the whole package down with it, so every test here dies at collection - the
    import statement is what proves importability, not this assertion. What this assertion adds
    is that all fifteen modules actually *registered*: importing a module and registering from it
    are separate events, and only the second is observable here. Weakening this to a subset or
    count check would discard exactly that, letting a backend that imported but never enrolled
    itself pass unnoticed.

    Distinct from its neighbours: two test classes earlier in this module prove label ordering for
    two named subsets of backends (the ones verifiable without external credentials, and the ones
    gated on them), and another test elsewhere in this module pins the curated tier's one member.
    This one pins the full registered set - every config that exists, not a subset of them - which
    is why it, and not either of those, is the assertion that fails when a new config is
    registered without a matching update here.
    """

    def test_registered_configs_equal_the_fifteen_in_label_order(self) -> None:
        from tests.integration.test_utils.data_source_config.clickhouse import (
            ClickHouseDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.mysql import (
            MySQLDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.oracle import (
            OracleDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.redshift import (
            RedshiftDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.sql_server import (
            SQLServerDatasourceTestConfig,
        )
        from tests.integration.test_utils.data_source_config.trino import (
            TrinoDatasourceTestConfig,
        )

        assert (
            BigQueryDatasourceTestConfig,  # big-query
            ClickHouseDatasourceTestConfig,  # clickhouse
            DatabricksDatasourceTestConfig,  # databricks
            SQLServerDatasourceTestConfig,  # mssql
            MySQLDatasourceTestConfig,  # mysql
            OracleDatasourceTestConfig,  # oracle
            PandasDataFrameDatasourceTestConfig,  # pandas-data-frame
            PandasFilesystemCsvDatasourceTestConfig,  # pandas-filesystem-csv
            PostgreSQLDatasourceTestConfig,  # postgresql
            RedshiftDatasourceTestConfig,  # redshift
            SingleStoreDatasourceTestConfig,  # singlestore
            SnowflakeDatasourceTestConfig,  # snowflake
            SparkFilesystemCsvDatasourceTestConfig,  # spark-filesystem-csv
            SqliteDatasourceTestConfig,  # sqlite
            TrinoDatasourceTestConfig,  # trino
        ) == _REGISTERED_CONFIGS


class TestDerivedTierListsReachEveryRegisteredConfig:
    """Guards `tiers.py`'s tier-keyed lists against a config that is declared and registered but
    never reaches `ALL_DATA_SOURCES`, `SQL_DATA_SOURCES` or `CURATED_SQL_DATA_SOURCES`, because
    its own module was imported after `tiers`'s in this package's `__init__.py`. Those lists are
    built once, at `tiers.py`'s own import time; a config that registers later is invisible to the
    already-built list even though the registry itself reports it correctly from then on, since
    `iter_data_source_configs`/`data_source_configs_for_tier` re-read the live registry on every
    call. This covers every tier-keyed list, not one of them, since all are built the same way and
    are equally exposed to the same import-order accident.
    """

    def test_all_data_sources_includes_every_registered_criterion_declaring_config(self) -> None:
        """`ALL_DATA_SOURCES` is the tier read itself, so it must reach every declaring config."""
        assert [type(config) for config in ALL_DATA_SOURCES] == list(
            _REGISTERED_CANONICAL_EXPECTATIONS
        )

    def test_sql_data_sources_includes_every_registered_sql_config(self) -> None:
        """`SQL_DATA_SOURCES` is that same tier read intersected with the SQL execution engine.

        The expected value is computed here from the captured tuple rather than re-read from the
        registry, for the reason the module comment above gives: inside this module's isolation
        seam a live registry read would see an emptied registry and pass vacuously.
        """
        assert [type(config) for config in SQL_DATA_SOURCES] == [
            config_class
            for config_class in _REGISTERED_CANONICAL_EXPECTATIONS
            if config_class.DATA_SOURCE_SPEC.execution_engine is ExecutionEngineKind.SQL
        ]

    def test_curated_sql_data_sources_includes_every_registered_curated_config(self) -> None:
        assert [type(config) for config in CURATED_SQL_DATA_SOURCES] == list(
            _REGISTERED_CURATED_SQL
        )


class TestDerivedEngineListsReachEveryRegisteredConfig:
    """The same guard as its tier-keyed neighbour above, over the two engine-keyed lists.

    Those two lists were literals until this work derived them, so nothing had ever had to check
    them against the registry. They are built the same way and at the same moment as the two
    tier-keyed lists — once, at `tiers.py`'s import time — so they are exposed to exactly the same
    import-order accident: a pandas or Spark config module imported after `tiers` registers too
    late to appear in the already-built list, while the accessor called here reports it correctly
    from then on. Keying on an execution engine rather than on a tier changes nothing about that
    exposure, so the guard has to cover all four lists rather than the two it started with.
    """

    def test_pandas_data_sources_includes_every_registered_pandas_config(self) -> None:
        assert [type(config) for config in PANDAS_DATA_SOURCES] == list(_REGISTERED_PANDAS_CONFIGS)

    def test_spark_data_sources_includes_every_registered_spark_config(self) -> None:
        assert [type(config) for config in SPARK_DATA_SOURCES] == list(_REGISTERED_SPARK_CONFIGS)


class TestRegisteredBackendDeclarationsSurviveAnAbsentDialectPackage:
    """This whole module runs in a lane that installs no third-party SQL dialect driver (no
    `pymysql`, no `psycopg2`, no Snowflake connector, and so on) - the same lane the registered-
    set membership pins above already run in. Those pins prove the package-level import succeeds
    with every dialect absent, since importing this module imports every backend module first;
    this test makes the *reason* that matters explicit rather than leaving it implicit in a pin
    whose primary purpose reads as something else.

    It catches a backend that reaches for its dialect driver at module scope without an import
    guard - which would already fail collection of this whole module - and, independently, a
    declared field whose value is only unsafe to touch once the instance exists (for example a
    property that dereferences the driver lazily instead of at class-body evaluation time, which
    an import failure alone would not catch). Instantiating with no arguments and reading every
    field is what exercises that second case; the table-schema-item field is checked for shape
    only; calling it is exactly the operation that would need the driver, so this test never does.

    What this deliberately does not cover: the *contents* of a `column_type_overrides` mapping
    built at module scope behind an import guard. That is a permitted shape for a backend whose
    override values come from its own driver - the same source line yields a populated mapping
    where the driver is installed and an empty one where it is not. Because no driver is installed
    in this lane, reading *such a mapping* here always observes its empty variant, which is the
    correct, intended behavior for *this* lane and says nothing about what it holds in a lane
    where the driver is present. An assertion pinning override contents belongs under that
    backend's own marker, in the lane that installs its driver.

    That scoping matters: it is a claim about the guarded shape, not about the field. A backend
    whose overrides come from core SQLAlchemy rather than from a driver declares the same mapping
    in every lane, so its entries are visible here too.
    """

    def test_no_argument_construction_and_full_field_read_raise_nothing(self) -> None:
        assert _REGISTERED_CONFIGS, "no SQL backends were registered to check"

        for config_class in _REGISTERED_CONFIGS:
            config = config_class()  # no arguments
            # Read through the shared accessor, not the SQL-only `backend_spec`: a registered
            # config need not be a SQL one, and the claim being made here - that no declared field
            # is unsafe to touch with no driver installed - is a property of every record.
            spec = config.data_source_spec

            for declared_field in fields(spec):
                getattr(spec, declared_field.name)  # every field; must not raise

            if isinstance(spec, SqlBackendSpec):
                # Checked for shape only. Calling it is exactly the operation that would require
                # the driver this lane does not install, so it is never invoked here.
                assert spec.table_schema_items is None or callable(spec.table_schema_items)


class TestDataSourceSpecMarkerResolution:
    """A record resolves its declared marker, and refuses to invent one it was not given.

    The refusal is the load-bearing half. Returning a placeholder mark for a record that declares
    no marker would let that record be parameterized into a suite, where the placeholder selects
    nothing and the run reports as passing - coverage that does not exist, reported as coverage
    that does. Raising turns that into a failure at the point the mistake was made.
    """

    def test_declared_marker_resolves_to_the_matching_mark_decorator(self) -> None:
        spec = DataSourceSpec(
            label="throwaway",
            public_name="Throwaway",
            provisioning=DataSourceProvisioning.IN_PROCESS,
            marker="sqlite",
        )

        assert spec.pytest_mark == pytest.mark.sqlite

    def test_absent_marker_raises_rather_than_returning_a_placeholder(self) -> None:
        spec = DataSourceSpec(
            label="throwaway",
            public_name="Throwaway",
            provisioning=DataSourceProvisioning.IN_PROCESS,
        )

        with pytest.raises(ValueError, match="throwaway"):
            _ = spec.pytest_mark


class TestDataSourceSpecConstruction:
    """Keyword-only and frozen, so a required field on a sub-record may follow defaulted fields."""

    def test_positional_construction_is_rejected(self) -> None:
        with pytest.raises(TypeError):
            DataSourceSpec(  # type: ignore[misc]
                "throwaway",
                "Throwaway",
                DataSourceProvisioning.IN_PROCESS,
            )

    def test_a_constructed_record_cannot_be_mutated(self) -> None:
        spec = DataSourceSpec(
            label="throwaway",
            public_name="Throwaway",
            provisioning=DataSourceProvisioning.IN_PROCESS,
        )

        with pytest.raises(FrozenInstanceError):
            spec.label = "other"  # type: ignore[misc]

    def test_only_the_first_three_fields_are_required(self) -> None:
        spec = DataSourceSpec(
            label="throwaway",
            public_name="Throwaway",
            provisioning=DataSourceProvisioning.IN_PROCESS,
        )

        assert spec.execution_engine is None
        assert spec.fluent_types == frozenset()
        assert spec.provisioning_note is None
        assert spec.marker is None
        assert spec.marker_scope is None
        assert spec.tiers == frozenset()
        assert spec.tier_case_exclusions == {}
        assert spec.ci_lane is None
        assert spec.dev_requirements_file is None
        assert spec.task_runner_marker is None
        assert spec.container_service is None


_ISOLATION_PROBE = """
import importlib.util
import sys

import pytest  # an allowed dependency of the record module, imported before the blocker

BLOCKED = {
    "tests",
    "great_expectations",
    "sqlalchemy",
    "pyspark",
    "py4j",
    "psycopg2",
    "pymysql",
    "pyodbc",
    "snowflake",
    "databricks",
    "trino",
    "oracledb",
    "cx_Oracle",
    "clickhouse_connect",
    "clickhouse_sqlalchemy",
    "singlestoredb",
    "redshift_connector",
    "google",
}


class _Blocker:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in BLOCKED:
            raise ImportError("blocked for this check: " + fullname)
        return None


sys.meta_path.insert(0, _Blocker())

# Loaded straight off the filesystem, with no package context at all. Importing it through its
# package would run the package __init__ and pull in every data source module, which is exactly
# what this check has to avoid: the claim is that this one module stands alone.
loader_spec = importlib.util.spec_from_file_location("_data_source_spec_isolated", sys.argv[1])
module = importlib.util.module_from_spec(loader_spec)
# dataclasses resolves a class's own module out of sys.modules while processing it, so the
# module has to be registered there before it executes, exactly as a normal import would.
sys.modules[loader_spec.name] = module
loader_spec.loader.exec_module(module)

record = module.DataSourceSpec(
    label="throwaway",
    public_name="Throwaway",
    provisioning=module.DataSourceProvisioning.IN_PROCESS,
    marker="sqlite",
)
assert record.pytest_mark == pytest.mark.sqlite

unmarked = module.DataSourceSpec(
    label="unmarked",
    public_name="Unmarked",
    provisioning=module.DataSourceProvisioning.IN_PROCESS,
)
try:
    unmarked.pytest_mark
except ValueError:
    pass
else:
    raise AssertionError("a record with no marker resolved a mark")

print("ok")
"""


class TestDataSourceSpecStandsAlone:
    """The record module is leftmost in the dependency direction, proved mechanically.

    Inspecting the import block would only show what the module names today. This runs the module
    in a process where the harness package, SQLAlchemy, every dialect driver and Spark all raise
    on import, and loads the file with no package context, so any dependency on them - now or
    later - fails the check rather than passing unnoticed.
    """

    def test_the_record_module_loads_with_no_harness_no_dialect_and_no_spark(self) -> None:
        module_path = Path(str(data_source_spec_module.__file__))

        completed = subprocess.run(
            [sys.executable, "-c", _ISOLATION_PROBE, str(module_path)],
            check=False,
            capture_output=True,
            text=True,
            cwd=tempfile.gettempdir(),  # away from the repo, so no path entry can shadow the block
        )

        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip() == "ok"


class TestRecordRegisteredWithoutAConfigClass:
    """A data source this repository declares but does not exercise still joins the registry.

    Requiring a config class in order to be registered would mean the only data sources that can
    be described are the ones the harness happens to run, which is precisely the gap that makes
    "what data sources exist" unanswerable from code. Storing the record alongside an *optional*
    config class is what lets a declaration-only data source be enumerable without inventing a
    config that no suite would ever drive.

    The mirror-image half matters just as much: such a record must stay out of every accessor
    that returns config classes. A consumer that parameterizes over configs would otherwise be
    handed a record it cannot instantiate, and the failure would surface far from the
    declaration that caused it.
    """

    def test_a_record_with_no_config_class_is_returned_by_the_record_accessor(self) -> None:
        spec = _make_core_spec(label="declaration-only")

        register_data_source(spec)

        assert iter_data_sources() == (RegisteredDataSource(spec=spec, config_class=None),)

    def test_a_record_with_no_config_class_is_returned_by_the_spec_accessor(self) -> None:
        spec = _make_core_spec(label="declaration-only")

        register_data_source(spec)

        assert iter_data_source_specs() == (spec,)

    def test_registration_returns_the_record_so_a_module_can_bind_it(self) -> None:
        spec = _make_core_spec(label="declaration-only")

        assert register_data_source(spec) is spec

    def test_a_record_with_no_config_class_is_absent_from_the_config_accessor(self) -> None:
        register_data_source(_make_core_spec(label="declaration-only"))

        assert iter_data_source_configs() == ()

    def test_a_record_with_no_config_class_is_absent_from_the_tier_accessor(self) -> None:
        register_data_source(
            _make_core_spec(
                label="declaration-only",
                tiers=frozenset({SupportTier.CURATED_SQL}),
                marker="declaration_only",
                ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="declaration_only"),
            )
        )

        assert data_source_configs_for_tier(SupportTier.CURATED_SQL) == ()

    def test_a_record_with_no_config_class_is_absent_from_the_engine_accessor(self) -> None:
        register_data_source(
            _make_core_spec(label="declaration-only", execution_engine=ExecutionEngineKind.SQL)
        )

        assert data_source_configs_for_engine(ExecutionEngineKind.SQL) == ()


class TestConfigBoundRegistrationIsStoredWithItsConfigClass:
    """A config-bound registration stores both halves, so one enumeration answers both questions.

    Keeping the record and its config class in one entry is what lets the config accessors be
    derived from the record set rather than maintained beside it. Two parallel stores would let
    the same data source be present in one and absent from the other.
    """

    def test_the_stored_entry_carries_both_the_record_and_the_class(self) -> None:
        spec = _make_spec(label="config-bound", marker="config_bound")
        config_class = _make_config_class("ConfigBound", spec)

        register_sql_config(config_class)

        assert iter_data_sources() == (RegisteredDataSource(spec=spec, config_class=config_class),)

    def test_the_stored_entry_reaches_the_spec_accessor(self) -> None:
        spec = _make_spec(label="config-bound", marker="config_bound")

        register_sql_config(_make_config_class("ConfigBound", spec))

        assert iter_data_source_specs() == (spec,)


class TestRecordAccessorsOrderByLabel:
    """Enumeration order is a property of the declarations, not of import order.

    A registry ordered by registration order would reorder itself whenever the import sorter
    moved a module, which turns an ordered pin into a test that fails for reasons unrelated to
    what it pins.
    """

    def test_iter_data_sources_orders_by_label_not_registration_order(self) -> None:
        zebra = _make_core_spec(label="zebra")
        apple = _make_core_spec(label="apple")

        register_data_source(zebra)
        register_data_source(apple)

        assert tuple(entry.spec for entry in iter_data_sources()) == (apple, zebra)

    def test_iter_data_source_specs_orders_by_label_not_registration_order(self) -> None:
        zebra = _make_core_spec(label="zebra")
        apple = _make_core_spec(label="apple")

        register_data_source(zebra)
        register_data_source(apple)

        assert iter_data_source_specs() == (apple, zebra)


class TestRecordAccessorsReadLiveState:
    """Every accessor reads the registry at call time.

    An accessor that captured its answer at import time would be correct only for consumers
    imported after every data source module, and silently short for every other one — the same
    import-order accident the derived lists already have to guard against.
    """

    def test_iter_data_sources_reflects_a_registration_made_after_an_earlier_call(self) -> None:
        assert iter_data_sources() == ()

        register_data_source(_make_core_spec(label="registered-later"))

        assert tuple(entry.spec.label for entry in iter_data_sources()) == ("registered-later",)

    def test_iter_data_source_specs_reflects_a_registration_made_after_an_earlier_call(
        self,
    ) -> None:
        assert iter_data_source_specs() == ()

        register_data_source(_make_core_spec(label="registered-later"))

        assert tuple(spec.label for spec in iter_data_source_specs()) == ("registered-later",)

    def test_engine_accessor_reflects_a_registration_made_after_an_earlier_call(self) -> None:
        assert data_source_configs_for_engine(ExecutionEngineKind.SQL) == ()

        config_class = _make_config_class(
            "LaterSql",
            _make_spec(
                label="registered-later",
                marker="registered_later",
                execution_engine=ExecutionEngineKind.SQL,
                tiers=_CANONICAL_CLAIM,
            ),
        )
        register_sql_config(config_class)

        assert data_source_configs_for_engine(ExecutionEngineKind.SQL) == (config_class,)


class TestConfigsForEngine:
    """Selecting configs by the engine that drives them.

    An engine-scoped consumer needs the configs one engine executes, and deriving that from the
    declaration is what keeps it from becoming a second hand-written list that drifts from the
    first.
    """

    def test_only_configs_declaring_that_engine_are_returned(self) -> None:
        sql_config = _make_config_class(
            "SqlDriven",
            _make_spec(
                label="sql-driven",
                marker="sql_driven",
                execution_engine=ExecutionEngineKind.SQL,
                tiers=_CANONICAL_CLAIM,
            ),
        )
        pandas_config = _make_config_class(
            "PandasDriven",
            _make_spec(
                label="pandas-driven",
                marker="pandas_driven",
                execution_engine=ExecutionEngineKind.PANDAS,
                tiers=_CANONICAL_CLAIM,
            ),
        )

        register_sql_config(sql_config)
        register_sql_config(pandas_config)

        assert data_source_configs_for_engine(ExecutionEngineKind.SQL) == (sql_config,)
        assert data_source_configs_for_engine(ExecutionEngineKind.PANDAS) == (pandas_config,)

    def test_a_config_declaring_no_engine_is_returned_for_no_engine(self) -> None:
        register_sql_config(
            _make_config_class("EngineLess", _make_spec(label="engine-less", marker="engine_less"))
        )

        assert data_source_configs_for_engine(ExecutionEngineKind.SQL) == ()
        assert data_source_configs_for_engine(ExecutionEngineKind.PANDAS) == ()
        assert data_source_configs_for_engine(ExecutionEngineKind.SPARK) == ()

    def test_results_are_ordered_by_label_not_registration_order(self) -> None:
        zebra = _make_config_class(
            "Zebra",
            _make_spec(
                label="zebra",
                marker="zebra_marker",
                execution_engine=ExecutionEngineKind.SQL,
                tiers=_CANONICAL_CLAIM,
            ),
        )
        apple = _make_config_class(
            "Apple",
            _make_spec(
                label="apple",
                marker="apple_marker",
                execution_engine=ExecutionEngineKind.SQL,
                tiers=_CANONICAL_CLAIM,
            ),
        )

        register_sql_config(zebra)
        register_sql_config(apple)

        assert data_source_configs_for_engine(ExecutionEngineKind.SQL) == (apple, zebra)


class TestIsolatedRegistryClearsRecordStorageBeforeYielding:
    """The seam clears the record storage on entry, not only on exit.

    A seam that merely restored afterwards would leave the body looking at a live view of
    whatever the surrounding process had already registered. Every assertion below is a
    *whole-registry equality* taken inside the seam, which is the only shape that distinguishes
    the two: it holds exactly when entering the seam emptied the storage, and fails as soon as a
    single record registered outside it remains visible within.
    """

    def test_a_record_registered_outside_the_seam_is_invisible_inside_it(self) -> None:
        register_data_source(_make_core_spec(label="outer"))
        outside = iter_data_sources()
        assert outside != ()

        with isolated_registry():
            assert iter_data_sources() == ()
            assert iter_data_source_specs() == ()

            register_data_source(_make_core_spec(label="inner"))

            assert tuple(entry.spec.label for entry in iter_data_sources()) == ("inner",)

        assert iter_data_sources() == outside

    def test_a_config_registered_outside_the_seam_is_invisible_inside_it(self) -> None:
        register_sql_config(
            _make_config_class(
                "Outer",
                _make_spec(
                    label="outer",
                    marker="outer",
                    execution_engine=ExecutionEngineKind.SQL,
                    tiers=_CANONICAL_CLAIM,
                ),
            )
        )
        outside = iter_data_source_configs()
        assert outside != ()

        with isolated_registry():
            assert iter_data_source_configs() == ()
            assert data_source_configs_for_engine(ExecutionEngineKind.SQL) == ()

        assert iter_data_source_configs() == outside


# Captured at this module's import time for the same reason as the tuples above: the autouse
# `_snapshot_registry` fixture clears the registry around every test, so a test body reading the
# live registry directly would see an empty one and pass vacuously. This one is the *record* view,
# which is strictly wider than the config view above: it includes the declarations this repository
# makes but does not exercise.
_REGISTERED_DATA_SOURCE_SPECS: Tuple[DataSourceSpec, ...] = iter_data_source_specs()

_REGISTERED_DATA_SOURCE_ENTRIES: Tuple[RegisteredDataSource, ...] = iter_data_sources()

# A declaration-only record is exactly an entry carrying no config class. Deriving the two views
# from the same snapshot, rather than subtracting one accessor's output from another's, is what
# makes them complementary by construction: an entry cannot be missing from both.
_DECLARATION_ONLY_SPECS: Mapping[str, DataSourceSpec] = {
    entry.spec.label: entry.spec
    for entry in _REGISTERED_DATA_SOURCE_ENTRIES
    if entry.config_class is None
}

_CONFIG_BOUND_LABELS: FrozenSet[str] = frozenset(
    entry.spec.label for entry in _REGISTERED_DATA_SOURCE_ENTRIES if entry.config_class is not None
)

_EXPECTED_DECLARATION_ONLY_SPECS: Mapping[str, DataSourceSpec] = {
    "alloydb": DataSourceSpec(
        label="alloydb",
        public_name="AlloyDB",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"alloy"}),
    ),
    "amazon-s3": DataSourceSpec(
        label="amazon-s3",
        public_name="Amazon S3",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"pandas_s3", "spark_s3"}),
        marker="aws_deps",
        marker_scope=MarkerScope.SHARED,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="aws_deps"),
        tiers=frozenset({SupportTier.FLUENT_API}),
        task_runner_marker="aws_deps",
    ),
    "aurora": DataSourceSpec(
        label="aurora",
        public_name="Amazon Aurora PostgreSQL",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"aurora"}),
    ),
    "azure-blob-storage": DataSourceSpec(
        label="azure-blob-storage",
        public_name="Azure Blob Storage",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"pandas_abs", "spark_abs"}),
    ),
    "citus": DataSourceSpec(
        label="citus",
        public_name="Citus",
        provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
        fluent_types=frozenset({"citus"}),
    ),
    "fabric": DataSourceSpec(
        label="fabric",
        public_name="Microsoft Fabric",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"fabric"}),
    ),
    "google-cloud-storage": DataSourceSpec(
        label="google-cloud-storage",
        public_name="Google Cloud Storage",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"pandas_gcs", "spark_gcs"}),
        marker="gcs_deps",
        marker_scope=MarkerScope.SHARED,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="gcs_deps"),
        tiers=frozenset({SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-gcs.txt",
        task_runner_marker="gcs_deps",
    ),
    "neon": DataSourceSpec(
        label="neon",
        public_name="Neon",
        provisioning=DataSourceProvisioning.EXTERNAL_CREDENTIALS,
        fluent_types=frozenset({"neon"}),
    ),
}


class TestDeclarationOnlyRecordsJoinTheRegistryWithoutConfigs:
    """The eight data sources this repository declares but does not exercise.

    Two halves, and both are load-bearing. They must reach the *record* accessors, or the
    declaration is unreachable and nothing downstream can read it; and they must reach *no*
    config accessor, or a consumer parameterizing over configs is handed a record it cannot
    instantiate. A test asserting only the first half would pass just as well for a broken
    registration that also leaked into the config view.
    """

    def test_the_record_accessor_holds_the_eight_declaration_only_labels(self) -> None:
        assert sorted(_DECLARATION_ONLY_SPECS) == [
            "alloydb",
            "amazon-s3",
            "aurora",
            "azure-blob-storage",
            "citus",
            "fabric",
            "google-cloud-storage",
            "neon",
        ]

    def test_no_declaration_only_label_reaches_the_config_accessor(self) -> None:
        """Read from the module-scope snapshot, not from a live call.

        The autouse isolation fixture clears the registry around every test in this module, so a
        body calling the accessor here would compare against an empty registry and pass no matter
        what was registered - which is exactly the vacuity this assertion exists to rule out.
        """
        assert _CONFIG_BOUND_LABELS.isdisjoint(_EXPECTED_DECLARATION_ONLY_SPECS)

    def test_the_record_accessor_is_wider_than_the_config_accessor_by_exactly_eight(self) -> None:
        assert (len(_REGISTERED_DATA_SOURCE_SPECS), len(_REGISTERED_CONFIGS)) == (23, 15)

    @pytest.mark.parametrize("label", sorted(_EXPECTED_DECLARATION_ONLY_SPECS))
    def test_every_declared_field_matches_the_reviewed_declaration(self, label: str) -> None:
        """One equality per record over every field but the free-text note.

        Comparing whole records rather than field-by-field is what makes this pin every field,
        including the ones a later edit might add: a record that gains an undeclared field fails
        here rather than drifting unpinned. The provisioning note is excluded and asserted
        separately, because pinning free prose to the character would fail on a rewording that
        changes nothing about what the record claims.
        """
        registered = _DECLARATION_ONLY_SPECS[label]

        assert (
            replace(registered, provisioning_note=None) == (_EXPECTED_DECLARATION_ONLY_SPECS[label])
        )

    def test_only_the_two_with_wiring_claim_a_tier(self) -> None:
        """A tier claim tracks whether a suite genuinely covers the record, not whether it is
        declaration-only.

        Six of the eight declare neither a marker nor a CI lane, so no suite in this repository
        runs against them and they claim no tier. Amazon S3 and Google Cloud Storage are the
        exception: they declare a real marker and CI lane, and their fluent types are exercised
        by the suite backing the fluent-API tier, so they claim that tier - while still being
        declaration-only in the sense this test class is named for, since neither has a harness
        config class. This is the assertion that keeps a declared CI lane from being conflated
        with a tier claim in the other direction: a lane means a job installs a data source's
        dependencies and runs something, and only a tier claim asserts that a specific suite
        passes against it.
        """
        assert {label: spec.tiers for label, spec in _DECLARATION_ONLY_SPECS.items()} == {
            "alloydb": frozenset(),
            "amazon-s3": frozenset({SupportTier.FLUENT_API}),
            "aurora": frozenset(),
            "azure-blob-storage": frozenset(),
            "citus": frozenset(),
            "fabric": frozenset(),
            "google-cloud-storage": frozenset({SupportTier.FLUENT_API}),
            "neon": frozenset(),
        }

    def test_citus_records_its_costed_onboarding_surfaces_in_its_provisioning_note(self) -> None:
        """Local-container provisioning with no container service is Citus's honest shape, and
        the note is where the cost of changing that is written down. Without it the declaration
        reads as an oversight rather than as a measured decision.
        """
        note = _DECLARATION_ONLY_SPECS["citus"].provisioning_note

        assert note is not None
        assert _DECLARATION_ONLY_SPECS["citus"].container_service is None
        for surface in (
            "marker",
            "REQUIRED_MARKERS",
            "requirements file",
            "MARKER_DEPENDENCY_MAP",
            "compose",
            "CI lane",
            "harness config",
        ):
            assert surface in note, f"the Citus provisioning note does not name {surface!r}"

    def test_fabric_records_the_service_principal_requirement_in_its_note(self) -> None:
        """A later effort scoping a Fabric lane starts from the actual authentication
        requirement rather than rediscovering it against a class that rejects every other mode.
        """
        note = _DECLARATION_ONLY_SPECS["fabric"].provisioning_note

        assert note is not None
        assert "service principal" in note.lower()


@pytest.fixture(scope="module", autouse=True)
def _the_real_registry_survives_this_module() -> Iterator[None]:
    """Every test here registers throwaway records; none of them may reach the real registry.

    The function-scoped `_snapshot_registry` fixture is what is supposed to guarantee that, and
    every test in this module trusts it. Nothing checked it end to end: a seam that restored the
    wrong thing, or restored nothing on one path, would leave the process's registry altered for
    every later test module in the same session — the wiring drift check among them — with no
    signal here at all. This teardown runs after the last function-scoped seam in this module has
    exited, and compares the registry against the snapshot taken at import time, before any test
    ran.
    """
    yield

    assert iter_data_sources() == _REGISTERED_DATA_SOURCE_ENTRIES, (
        "this module left the real registry altered; a throwaway registration escaped the "
        "isolation seam"
    )


class TestRegisteredRecordsEqualTheTwentyThreeInLabelOrder:
    """The second half of the registered-set pin: every registered *record*, in label order.

    Its neighbour `TestRegisteredConfigsEqualTheFifteenInLabelOrder` pins the registered *config
    classes*, and keeps its exact shape and teeth. That set is strictly narrower than the registry:
    a record registered without a config class reaches no config accessor at all, so eight of the
    twenty-three declarations this repository makes are invisible to that pin. They are not
    invisible to the consumers that matter — the wiring drift check and the generated compatibility
    reference both walk records, not configs — so a record dropped, relabelled, or never registered
    would change what those produce while the config pin stayed green.

    Both pins are ordered whole-set equalities, and neither may be weakened into a subset, a
    membership check, or a count. That shape is the entire point: adding a data source has to fail
    in the same change that adds it. `test_the_record_accessor_is_wider_than_the_config_accessor_by
    _exactly_eight` is a count and is not a substitute — a count passes for any twenty-three labels,
    including twenty-two correct ones and a typo.

    Labels rather than record objects, deliberately: the record objects' every field is pinned
    elsewhere — the eight declaration-only records by whole-record equality against a reviewed
    literal, the config-bound ones through their configs — while what is pinned here is exactly
    which data sources are enrolled and in what order.
    """

    def test_registered_record_labels_equal_the_twenty_three_in_label_order(self) -> None:
        assert tuple(spec.label for spec in _REGISTERED_DATA_SOURCE_SPECS) == (
            "alloydb",
            "amazon-s3",
            "aurora",
            "azure-blob-storage",
            "big-query",
            "citus",
            "clickhouse",
            "databricks",
            "fabric",
            "google-cloud-storage",
            "mssql",
            "mysql",
            "neon",
            "oracle",
            "pandas-data-frame",
            "pandas-filesystem-csv",
            "postgresql",
            "redshift",
            "singlestore",
            "snowflake",
            "spark-filesystem-csv",
            "sqlite",
            "trino",
        )


def _resolves_to(config_class: type, method_name: str) -> str:
    """The name of the class whose body actually supplies `method_name` for `config_class`."""
    for ancestor in config_class.__mro__:
        if method_name in vars(ancestor):
            return ancestor.__name__
    raise AssertionError(f"{config_class.__name__} resolves no {method_name}")


# Captured from the tree as it stood before this work — `origin/develop` at
# b94159e0d5899192dca365e47289671ab94ecafc — by resolving `__eq__` and `__hash__` through each
# registered config's MRO there. Every entry is a literal, not a derivation: a pin that recomputed
# the expected value from the same objects it checks would agree with itself no matter what those
# objects became.
_BASELINE_EQ_AND_HASH_RESOLUTION: Mapping[str, Tuple[str, str]] = {
    "BigQueryDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "ClickHouseDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "DatabricksDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "MySQLDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "OracleDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "PandasDataFrameDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "PandasFilesystemCsvDatasourceTestConfig": (
        "PandasFilesystemCsvDatasourceTestConfig",
        "PandasFilesystemCsvDatasourceTestConfig",
    ),
    "PostgreSQLDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "RedshiftDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "SQLServerDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "SingleStoreDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "SnowflakeDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "SparkFilesystemCsvDatasourceTestConfig": (
        "SparkFilesystemCsvDatasourceTestConfig",
        "SparkFilesystemCsvDatasourceTestConfig",
    ),
    "SqliteDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
    "TrinoDatasourceTestConfig": ("DataSourceTestConfig", "DataSourceTestConfig"),
}


class TestEveryRegisteredConfigResolvesTheSameEqualityAndHashAsBeforeThisWork:
    """A **no-change** assertion, and the distinction from its neighbour is the whole point.

    `TestRegisteredBackendsKeepTheInheritedHash` asserts the *stronger* form: that a config's
    `__eq__` and `__hash__` **are** the shared base's. That is the right rule where it is applied —
    over `SqlDatasourceTestConfig`'s descendants, none of which carries a field the base's
    hand-written, mapping-safe implementations do not already handle — and it is sitting right
    here, which makes reusing it over every registered config look like the obvious move.

    Applied to every registered config it would mandate a wrong-data defect. The pandas and Spark
    filesystem-CSV configs each re-declare themselves as a frozen dataclass to add their read and
    write option mappings, which regenerates equality and hash on the subclass. Their generated
    equality compares every field, the option mappings included — which is the behavior those two
    configs exist to have. The inherited implementation compares the test label and the pytest mark
    and nothing else, so under it two instances differing only in read options compare **equal**;
    the session-scoped batch-setup cache keys on config equality, so the second would silently
    reuse the first's setup and read its CSVs with the wrong options. A live suite constructs
    exactly that pair (`tests/integration/data_sources_and_expectations/
    test_expectation_conditions.py` builds a date-parsing-configured filesystem config alongside a
    default one).

    So the assertion this makes is not "resolves to the base's" but "resolves to what it resolved to
    before this work", against a literal captured from the baseline. That is what proves the
    retrofit was additive — it added a class attribute and deleted two hand-written properties, and
    touched no `@dataclass` decoration — and it fails if a future change re-decorates any config in
    either direction: opting one of the two filesystem configs out of generated equality reddens it
    just as surely as regenerating equality on one of the inherited thirteen would.

    No hash *value* is pinned, only the resolution and (elsewhere, against a control) the behavior.
    `test_id` is a `str` and `PYTHONHASHSEED` is randomized, so a hash value is not reproducible
    from one process to the next and a pin on one would be a flake, not a guard.
    """

    def test_every_registered_config_resolves_the_baseline_implementations(self) -> None:
        assert _REGISTERED_CONFIGS, "no registered configs were found to check"

        resolved = {
            config_class.__name__: (
                _resolves_to(config_class, "__eq__"),
                _resolves_to(config_class, "__hash__"),
            )
            for config_class in _REGISTERED_CONFIGS
        }

        assert resolved == dict(_BASELINE_EQ_AND_HASH_RESOLUTION)


class _PandasDataFrameControlConfig(DataSourceTestConfig):
    """The in-memory pandas config as it was written before it declared a record: `label` and
    `pytest_mark` hand-coded, and — like the config it controls for — no re-declaration as a
    dataclass, so it inherits the shared base's mapping-safe equality and hash.
    """

    @property
    @override
    def label(self) -> str:
        return "pandas-data-frame"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.unit

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        raise NotImplementedError("not exercised by these tests")


@dataclass(frozen=True)
class _PandasFilesystemCsvControlConfig(DataSourceTestConfig):
    """The pandas filesystem-CSV config as it was written before it declared a record.

    The re-declaration as a frozen dataclass carrying the two option mappings is not incidental to
    this control — it is the thing being controlled for. It regenerates equality and hash exactly
    as the real config's does, so the control shares the real config's equality semantics *and* its
    recorded latent hashing defect rather than papering over either.
    """

    read_options: Dict[str, Any] = field(default_factory=dict)
    write_options: Dict[str, Any] = field(default_factory=dict)

    @property
    @override
    def label(self) -> str:
        return "pandas-filesystem-csv"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.filesystem

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        raise NotImplementedError("not exercised by these tests")


@dataclass(frozen=True)
class _SparkFilesystemCsvControlConfig(DataSourceTestConfig):
    """The Spark filesystem-CSV config as it was written before it declared a record, re-declared
    as a frozen dataclass carrying the two option mappings for the same reason as the pandas one.
    """

    read_options: Dict[str, Any] = field(default_factory=dict)
    write_options: Dict[str, Any] = field(default_factory=dict)

    @property
    @override
    def label(self) -> str:
        return "spark-filesystem-csv"

    @property
    @override
    def pytest_mark(self) -> pytest.MarkDecorator:
        return pytest.mark.spark

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        raise NotImplementedError("not exercised by these tests")


# What each retrofitted config's record is expected to declare, written out here rather than read
# back off the config. Reading the declaration back would compare the record to itself; every field
# below is a reviewed value, so a change to any one of them — the public name a generated document
# prints, the execution engine that decides which derived list the config joins, the fluent types
# the wiring check resolves, the marker scope that decides whether the marker is checked for
# collision, or either half of the CI lane — fails here.
_RetrofitControl = Tuple[Type[DataSourceTestConfig], Type[DataSourceTestConfig], DataSourceSpec]

_RETROFITTED_CONTROLS: Mapping[str, _RetrofitControl] = {
    "pandas-data-frame": (
        PandasDataFrameDatasourceTestConfig,
        _PandasDataFrameControlConfig,
        DataSourceSpec(
            label="pandas-data-frame",
            public_name="Pandas",
            provisioning=DataSourceProvisioning.IN_PROCESS,
            execution_engine=ExecutionEngineKind.PANDAS,
            fluent_types=frozenset({"pandas"}),
            marker="unit",
            marker_scope=MarkerScope.SHARED,
            ci_lane=CiLaneRef(workflow_job="unit-tests", marker_token="unit"),
            tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        ),
    ),
    "pandas-filesystem-csv": (
        PandasFilesystemCsvDatasourceTestConfig,
        _PandasFilesystemCsvControlConfig,
        DataSourceSpec(
            label="pandas-filesystem-csv",
            public_name="Pandas",
            provisioning=DataSourceProvisioning.LOCAL_FILE,
            execution_engine=ExecutionEngineKind.PANDAS,
            fluent_types=frozenset({"pandas_filesystem"}),
            marker="filesystem",
            marker_scope=MarkerScope.SHARED,
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="filesystem"),
            tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        ),
    ),
    "spark-filesystem-csv": (
        SparkFilesystemCsvDatasourceTestConfig,
        _SparkFilesystemCsvControlConfig,
        DataSourceSpec(
            label="spark-filesystem-csv",
            public_name="Spark",
            provisioning=DataSourceProvisioning.LOCAL_FILE,
            execution_engine=ExecutionEngineKind.SPARK,
            fluent_types=frozenset({"spark_filesystem"}),
            marker="spark",
            marker_scope=MarkerScope.SHARED,
            ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="spark"),
            tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
            dev_requirements_file="reqs/requirements-dev-spark.txt",
            task_runner_marker="spark",
        ),
    ),
}

_OPTION_CARRYING_LABELS = ("pandas-filesystem-csv", "spark-filesystem-csv")


class TestRetrofittedConfigsMatchAHandWrittenControl:
    """The three non-SQL configs, each against a control declaring an equivalent record.

    This is the pattern the SQL half of this module already uses — `_HandWrittenControlConfig`
    against `_DeclaredConfig` — applied to the three configs the retrofit rewrote. It lives here
    rather than beside each config because the control-config pattern and the isolation seam both
    live in this module.

    Without it, the three records' declared fields are pinned by nothing at all: change any of
    `public_name`, `execution_engine`, `fluent_types`, `marker_scope`, or either half of a CI lane
    on any of the three, and every other test in the repository stays green.
    """

    @pytest.mark.parametrize("label", sorted(_RETROFITTED_CONTROLS))
    def test_declares_exactly_the_reviewed_record(self, label: str) -> None:
        config_class, _, expected_spec = _RETROFITTED_CONTROLS[label]

        assert expected_spec == config_class.DATA_SOURCE_SPEC

    @pytest.mark.parametrize("label", sorted(_RETROFITTED_CONTROLS))
    def test_reports_the_controls_label_mark_and_test_id(self, label: str) -> None:
        config_class, control_class, _ = _RETROFITTED_CONTROLS[label]
        config = config_class()
        control = control_class()

        assert config.label == control.label == label
        assert config.pytest_mark == control.pytest_mark
        assert config.test_id == control.test_id

    def test_the_data_frame_config_compares_and_hashes_exactly_as_its_control(self) -> None:
        """The one of the three that is not re-declared, so it inherits the base's implementations.

        Cross-class equality is meaningful here precisely because both sides resolve the same
        inherited `__eq__`, which compares label and mark rather than class identity — and the
        hashes agree for the same reason.
        """
        config = PandasDataFrameDatasourceTestConfig()
        control = _PandasDataFrameControlConfig()

        assert config == control
        assert hash(config) == hash(control)

    @pytest.mark.parametrize("label", _OPTION_CARRYING_LABELS)
    def test_the_option_carrying_configs_compare_on_their_options_like_their_controls(
        self, label: str
    ) -> None:
        """Generated equality, on both the config and its control, discriminates the options.

        Cross-class equality is *not* asserted for these two, and its absence is the correct
        behavior rather than a gap: generated equality returns `NotImplemented` for an instance of
        another class, so a config and its control are unequal no matter how equivalent their
        records are. What a control can attest to here is the semantics — that two instances of the
        same class differing only in read options compare unequal — and it attests to it by
        exhibiting the same semantics from a class written the way the config was written before
        this work.
        """
        config_class, control_class, _ = _RETROFITTED_CONTROLS[label]

        for cls in (config_class, control_class):
            assert cls() == cls()
            assert cls(read_options={"parse_dates": ["d"]}) != cls()  # type: ignore[call-arg]
            assert cls(read_options={"parse_dates": ["d"]}) == cls(  # type: ignore[call-arg]
                read_options={"parse_dates": ["d"]}
            )

    @pytest.mark.parametrize("label", _OPTION_CARRYING_LABELS)
    def test_the_option_carrying_configs_are_unhashable_exactly_as_their_controls_are(
        self, label: str
    ) -> None:
        """The recorded latent defect, pinned as behavior rather than fixed.

        Re-declaring these two as frozen dataclasses regenerates `__hash__`, and the generated one
        hashes `extra_column_types` — a `dict` — raising `TypeError` on every instance. Nothing in
        the repository hashes either config today, so the defect is latent, and it predates this
        work: the control, written the way the config was written before the retrofit, raises the
        same way. **The obvious remedy — opting out of generated equality so the mapping-safe
        inherited implementations are used — is a wrong-data defect here**, because it would also
        widen equality to ignore the option mappings and collapse two differently-configured
        instances into one batch-setup cache entry. This assertion exists so that a maintainer who
        rediscovers the unhashability finds it recorded as known, alongside the reason its obvious
        fix is not applied.
        """
        config_class, control_class, _ = _RETROFITTED_CONTROLS[label]

        for cls in (config_class, control_class):
            with pytest.raises(TypeError, match="unhashable type"):
                hash(cls())


class TestEveryRegisteringModuleIsImportedBeforeTheDerivedLists:
    """The import-order guarantee, extended past the modules a derived list can observe.

    `TestDerivedTierListsReachEveryRegisteredConfig` and its engine-keyed sibling catch an
    ordering violation by its consequence: a config registered after `tiers.py` is missing from a
    list that `tiers.py` already built. That works only for a module whose registrations reach a
    derived list. `declaration_only.py` registers eight records that carry no config class, so they
    reach no derived list at all — move its import below `tiers`'s and every one of those
    assertions stays green while the eight records finish registering later than the package's own
    documented ordering promises.

    That promise is what the package `__init__` states in prose and enforces with an `isort: split`
    that only holds while a new import lands *above* the split. This reads the ordering out of the
    `__init__` itself, so it holds for a module whose registrations are invisible downstream, and
    for one added tomorrow.
    """

    @staticmethod
    def _package_dir() -> Path:
        return Path(data_source_spec_module.__file__).parent

    @classmethod
    def _registering_module_names(cls) -> FrozenSet[str]:
        """Every module in the package that enrols something when it is imported.

        Found by reading the sources rather than by listing them here: a list would have to be
        extended by hand by the same change that adds a module, which is the maintenance-by-hand
        this whole effort exists to remove. `registry` itself is excluded because it *defines*
        these names rather than calling them.
        """
        registering = set()
        for source_path in cls._package_dir().glob("*.py"):
            if source_path.stem in {"__init__", "registry"}:
                continue
            source = source_path.read_text()
            if any(
                token in source
                for token in (
                    "@register_sql_config",
                    "@register_data_source_config",
                    "register_data_source(",
                )
            ):
                registering.add(source_path.stem)
        return frozenset(registering)

    @classmethod
    def _relative_import_order(cls) -> List[str]:
        tree = ast.parse((cls._package_dir() / "__init__.py").read_text())
        return [
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.level == 1 and node.module is not None
        ]

    def test_the_declaration_only_module_is_imported_before_tiers(self) -> None:
        order = self._relative_import_order()

        assert order.index("declaration_only") < order.index("tiers")

    def test_every_registering_module_is_imported_before_tiers(self) -> None:
        registering = self._registering_module_names()
        assert "declaration_only" in registering, (
            "the declaration-only module no longer registers anything; this guard is looking at "
            "the wrong module"
        )
        assert len(registering) >= 15, (
            f"only {len(registering)} registering modules were found; the source scan that finds "
            f"them has probably stopped matching"
        )

        order = self._relative_import_order()
        tiers_position = order.index("tiers")
        late = sorted(
            name for name in registering if name not in order or order.index(name) > tiers_position
        )

        assert late == [], (
            f"{late} register data sources but are imported at or after `tiers`, which builds "
            f"every derived list at its own import time and cannot see a registration made later"
        )


class TestEveryRegisteredRecordSurvivesAnAbsentDialectPackage:
    """The dialect-absent guard, widened from the registered configs to every registered record.

    Its narrower neighbour reads every field of every *config-bound* record and constructs every
    config with no argument. Eight registered records carry no config class, so that guard never
    reads them — and a record is exactly the kind of object that can hide a lazy dereference of a
    package this lane does not install, because a field can be a property, a factory, or a value
    built at module scope behind an import guard.

    Same lane, same claim: this module runs with no SQL dialect driver and no Spark distribution
    installed, so every record's every field being readable here is the invariant that a record
    module imports and answers questions without its own data source's driver present.
    """

    def test_every_field_of_every_registered_record_reads_without_a_driver(self) -> None:
        assert _REGISTERED_DATA_SOURCE_ENTRIES, "no records were registered to check"

        for entry in _REGISTERED_DATA_SOURCE_ENTRIES:
            for declared_field in fields(entry.spec):
                getattr(entry.spec, declared_field.name)  # must not raise

    def test_every_registered_config_constructs_with_no_argument(self) -> None:
        constructed = [
            entry.config_class()
            for entry in _REGISTERED_DATA_SOURCE_ENTRIES
            if entry.config_class is not None
        ]

        assert len(constructed) == len(_REGISTERED_CONFIGS)


class TestMarkerScopeRelaxationsProvenWithThrowawayRecords:
    """The shared-marker and absent-marker relaxations, proven where they can actually be observed.

    Every case below builds throwaway records inside the isolation seam, and that is a requirement
    rather than a convenience. **No two records this work registers share a marker at all**, so an
    assertion taken over the real registry would pass identically whether the relaxation works or
    whether `_dedicated_marker` returned every marker unconditionally — it would be a test of the
    registered set's happening to have no collisions, not of the rule. The relaxation exists so
    that a later criteria suite can parameterize over data sources sharing a dependency-class
    marker (`aws_deps`, `spark`, `filesystem`) without this record schema being reopened, and that
    future is exactly what a real-registry assertion cannot reach.

    The last case below pins the direction the rule reads an *undeclared* scope in, and it is
    load-bearing in the opposite way to the rest: reading undeclared as shared would make the
    duplicate-marker rule vacuous for every record registered today, since not one of the twelve
    SQL records declares a scope — and every other test in this module would still pass.
    """

    def test_two_records_sharing_one_marker_both_declared_shared_register_cleanly(self) -> None:
        register_data_source(
            _make_core_spec(label="first", marker="shared_dep", marker_scope=MarkerScope.SHARED)
        )
        register_data_source(
            _make_core_spec(label="second", marker="shared_dep", marker_scope=MarkerScope.SHARED)
        )

        assert sorted(spec.label for spec in iter_data_source_specs()) == ["first", "second"]

    def test_two_records_sharing_one_marker_both_declared_dedicated_are_rejected_naming_both(
        self,
    ) -> None:
        register_data_source(
            _make_core_spec(
                label="first", marker="dedicated_dep", marker_scope=MarkerScope.DEDICATED
            )
        )

        with pytest.raises(ValueError) as excinfo:
            register_data_source(
                _make_core_spec(
                    label="second", marker="dedicated_dep", marker_scope=MarkerScope.DEDICATED
                )
            )

        message = str(excinfo.value)
        assert "dedicated_dep" in message
        assert "'first'" in message
        assert "'second'" in message

    def test_one_dedicated_and_one_shared_record_sharing_a_marker_register_cleanly(self) -> None:
        """Both orders, because the rule indexes only the dedicated claimant.

        Registering the dedicated one first and the shared one second exercises the branch that
        declines to *look* the marker up; the reverse order exercises the branch that declines to
        *store* it. A single order would leave one of the two unproven.
        """
        register_data_source(
            _make_core_spec(label="ded-first", marker="mixed_a", marker_scope=MarkerScope.DEDICATED)
        )
        register_data_source(
            _make_core_spec(label="shr-second", marker="mixed_a", marker_scope=MarkerScope.SHARED)
        )
        register_data_source(
            _make_core_spec(label="shr-first", marker="mixed_b", marker_scope=MarkerScope.SHARED)
        )
        register_data_source(
            _make_core_spec(
                label="ded-second", marker="mixed_b", marker_scope=MarkerScope.DEDICATED
            )
        )

        assert sorted(spec.label for spec in iter_data_source_specs()) == [
            "ded-first",
            "ded-second",
            "shr-first",
            "shr-second",
        ]

    def test_two_records_declaring_no_marker_at_all_register_cleanly(self) -> None:
        """Absence is not a duplicated value. Two records with no marker are not two records
        sharing one."""
        register_data_source(_make_core_spec(label="first", marker=None))
        register_data_source(_make_core_spec(label="second", marker=None))

        assert sorted(spec.label for spec in iter_data_source_specs()) == ["first", "second"]

    def test_an_undeclared_scope_is_read_as_dedicated_and_still_collides(self) -> None:
        register_data_source(_make_core_spec(label="first", marker="undeclared_scope"))

        with pytest.raises(ValueError) as excinfo:
            register_data_source(_make_core_spec(label="second", marker="undeclared_scope"))

        assert "undeclared_scope" in str(excinfo.value)


class TestTierClaimsScaleTheObligationsProvenInBothDirections:
    """Each obligation a tier claim creates, proven to fire under the claim and not without it.

    Only the rejection half was ever exercised permanently, and a rejection alone does not
    distinguish "this obligation is scaled to the claim" from "this obligation applies to every
    record". The second half of each pair — the identical record with no tier claimed, registering
    cleanly — is what makes the claim-scaling itself the thing under test. Membership in no tier is
    a valid, honest declaration meaning "this data source ships, but no tier's suite proves it",
    and a record making it owes no marker, no lane and no container service; if any of these three
    obligations leaked into the unclaimed case, that declaration would become unexpressible and the
    eight declaration-only records could not be registered at all.

    Throwaway records again, and for the same reason as the marker relaxations: every registered
    record either claims a tier and satisfies all three obligations or claims none, so the real
    registry cannot tell a working scaling rule from an unconditional one.
    """

    _CLAIM = frozenset({SupportTier.CANONICAL_EXPECTATIONS})

    def test_a_tier_claim_with_no_marker_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="declares no data source marker"):
            register_data_source(_make_core_spec(marker=None, tiers=self._CLAIM))

    def test_the_same_record_claiming_no_tier_registers_cleanly(self) -> None:
        register_data_source(_make_core_spec(marker=None))

        assert [spec.label for spec in iter_data_source_specs()] == ["throwaway-core"]

    def test_a_tier_claim_with_no_ci_lane_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="declares no CI lane"):
            register_data_source(
                _make_core_spec(marker="throwaway", ci_lane=None, tiers=self._CLAIM)
            )

    def test_the_same_record_with_a_marker_and_no_lane_claiming_no_tier_registers_cleanly(
        self,
    ) -> None:
        register_data_source(_make_core_spec(marker="throwaway", ci_lane=None))

        assert [spec.label for spec in iter_data_source_specs()] == ["throwaway-core"]

    def test_a_tier_claim_with_local_container_provisioning_and_no_service_is_rejected(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="names no container_service"):
            register_data_source(
                _make_core_spec(
                    marker="throwaway",
                    ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="throwaway"),
                    provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
                    container_service=None,
                    tiers=self._CLAIM,
                )
            )

    def test_the_same_containerized_record_claiming_no_tier_registers_cleanly(self) -> None:
        """Citus's shape exactly: a data source distributed as a container image this repository
        has no compose file for. Running it locally is how you would reach it, and this repository
        does not, so it claims no tier and names no service."""
        register_data_source(
            _make_core_spec(
                marker="throwaway",
                ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="throwaway"),
                provisioning=DataSourceProvisioning.LOCAL_CONTAINER,
                container_service=None,
            )
        )

        assert [spec.label for spec in iter_data_source_specs()] == ["throwaway-core"]


# --- The shared canonical expectation parameterization is mandatory -------------------------
#
# Registration rejects a record that has a config class and a declared execution engine and does
# not declare the shared canonical expectation parameterization criterion, unless its label is in
# `_OUTSIDE_SHARED_PARAMETERIZATION` with the reason it sits out. The two helpers below are the
# literal's liveness checks, written as pure functions over an exemption mapping and a record
# snapshot so that each can be handed a deliberately broken literal and shown to fail — a check
# that has only ever been run against a literal that satisfies it is a check nobody has seen work.
#
# The generic-SQL escape hatch never reaches any of this, because it is never registered:
# `generic_sql.py` declares a `public_name` and a `DATA_SOURCE_SPEC` but carries no registration
# decorator, so it is not a record. It belongs to both hand-written shared lists, so a reader who
# knows that and does not know it is unregistered will read its absence from the exemption literal
# as a bug. It is not one — see
# `TestGenericSqlEscapeHatchIsDeclaredButUnregistered` above, which attempts the registration that
# never happens and pins that the rule would reject it.


def _exemptions_naming_no_registered_record(
    exemptions: Mapping[str, str], specs: Sequence[DataSourceSpec]
) -> Tuple[str, ...]:
    """Exempted labels no registered record answers to.

    A stale entry naming a data source this repository no longer declares exempts nothing, and
    reads to the next maintainer as evidence that the data source still exists.
    """
    labels = {spec.label for spec in specs}
    return tuple(sorted(label for label in exemptions if label not in labels))


def _exemptions_naming_a_declaring_record(
    exemptions: Mapping[str, str], specs: Sequence[DataSourceSpec]
) -> Tuple[str, ...]:
    """Exempted labels whose record does declare the criterion after all.

    An exemption for a record that has since joined the shared parameterization is an exemption
    that has outlived its reason. Left in place it costs nothing today and silently re-authorizes
    the opt-out the day someone drops the declaration.
    """
    return tuple(
        sorted(
            spec.label
            for spec in specs
            if spec.label in exemptions and SupportTier.CANONICAL_EXPECTATIONS in spec.tiers
        )
    )


class TestSharedParameterizationDeclarationIsMandatoryProvenWithThrowawayRecords:
    """The mandatory-declaration rule, proven where it can actually be observed.

    Every case below builds throwaway records inside the isolation seam, and that is a
    requirement rather than a convenience. **After the three non-SQL retrofits, every registered
    config either declares the criterion or is in the exemption literal**, so an assertion taken
    over the real registry would pass identically whether the rule fires or whether the validator
    returned unconditionally — it would be a test of the registered set's happening to comply, not
    of the rule. The near-miss case is what makes the exemption itself the thing under test: the
    same record, differing only in whether its label is exempt, is rejected in one case and
    admitted in the other.
    """

    _EXEMPT_LABEL = "clickhouse"

    def test_a_config_bound_record_with_an_engine_and_no_criterion_is_rejected(self) -> None:
        with pytest.raises(ValueError) as excinfo:
            register_sql_config(
                _make_config_class(
                    "Silent",
                    _make_spec(
                        label="silent-opt-out",
                        marker="silent_opt_out",
                        execution_engine=ExecutionEngineKind.SQL,
                    ),
                )
            )

        message = str(excinfo.value)
        # The branch, not a substring another rejection could satisfy: the phrase below appears in
        # this rejection and no other, and the label and criterion pin which record and which
        # criterion the rejection is about.
        assert "does not declare the shared canonical expectation parameterization" in message
        assert "'silent-opt-out'" in message
        assert "'canonical_expectations'" in message

    def test_the_same_record_registers_cleanly_once_it_declares_the_criterion(self) -> None:
        register_sql_config(
            _make_config_class(
                "Declaring",
                _make_spec(
                    label="silent-opt-out",
                    marker="silent_opt_out",
                    execution_engine=ExecutionEngineKind.SQL,
                    tiers=_CANONICAL_CLAIM,
                ),
            )
        )

        assert [spec.label for spec in iter_data_source_specs()] == ["silent-opt-out"]

    def test_the_same_record_registers_cleanly_when_its_label_is_exempt(self) -> None:
        """The near-miss: identical in every respect to the rejected record except its label,
        which the exemption literal names. This is what proves the exemption is what admits it —
        a record admitted for any other reason would be admitted under the rejected label too."""
        assert self._EXEMPT_LABEL in _OUTSIDE_SHARED_PARAMETERIZATION

        register_sql_config(
            _make_config_class(
                "Exempt",
                _make_spec(
                    label=self._EXEMPT_LABEL,
                    marker="silent_opt_out",
                    execution_engine=ExecutionEngineKind.SQL,
                ),
            )
        )

        assert [spec.label for spec in iter_data_source_specs()] == [self._EXEMPT_LABEL]

    def test_a_record_with_no_config_class_is_not_subject_to_the_rule(self) -> None:
        """A declaration-only record has no config to instantiate and runs in no suite. Dragging
        it into the shared parameterization would be advertising coverage that cannot exist."""
        register_data_source(
            _make_core_spec(label="declaration-only", execution_engine=ExecutionEngineKind.SQL)
        )

        assert [spec.label for spec in iter_data_source_specs()] == ["declaration-only"]

    def test_a_config_declaring_no_execution_engine_is_not_subject_to_the_rule(self) -> None:
        """A config naming no engine is a record the derived engine lists cannot place either.
        The rule leaves it to the well-formedness checks rather than inventing a rule for it."""
        register_sql_config(
            _make_config_class(
                "EngineLess",
                _make_spec(label="engine-less", marker="engine_less"),
            )
        )

        assert [spec.label for spec in iter_data_source_specs()] == ["engine-less"]


class TestTheExemptionLiteralIsKeptLive:
    """The two checks that stop the exemption literal becoming a place exemptions accumulate.

    Both are read from the module-scope record snapshot rather than the live registry: the autouse
    `_snapshot_registry` fixture clears the registry around every test, so a body reading the live
    accessors would check an empty set and pass vacuously.

    Each check is demonstrated able to fail immediately below the assertion it backs, by handing
    the same function a literal extended with exactly the entry it is meant to catch.
    """

    def test_the_literal_names_exactly_the_four_curated_backends_with_a_reason_each(self) -> None:
        assert sorted(_OUTSIDE_SHARED_PARAMETERIZATION) == [
            "clickhouse",
            "oracle",
            "singlestore",
            "trino",
        ]
        assert all(reason.strip() for reason in _OUTSIDE_SHARED_PARAMETERIZATION.values())

    def test_every_exempted_label_resolves_to_a_registered_record(self) -> None:
        assert (
            _exemptions_naming_no_registered_record(
                _OUTSIDE_SHARED_PARAMETERIZATION, _REGISTERED_DATA_SOURCE_SPECS
            )
            == ()
        )

    def test_that_check_fails_on_a_literal_naming_an_unregistered_label(self) -> None:
        stale = {**_OUTSIDE_SHARED_PARAMETERIZATION, "retired_backend": "removed years ago"}

        assert _exemptions_naming_no_registered_record(stale, _REGISTERED_DATA_SOURCE_SPECS) == (
            "retired_backend",
        )

    def test_no_exempted_label_declares_the_criterion(self) -> None:
        assert (
            _exemptions_naming_a_declaring_record(
                _OUTSIDE_SHARED_PARAMETERIZATION, _REGISTERED_DATA_SOURCE_SPECS
            )
            == ()
        )

    def test_that_check_fails_on_a_literal_naming_a_record_that_declares_the_criterion(
        self,
    ) -> None:
        declaring = {
            **_OUTSIDE_SHARED_PARAMETERIZATION,
            "postgresql": "exemption outlived its reason",
        }

        assert _exemptions_naming_a_declaring_record(declaring, _REGISTERED_DATA_SOURCE_SPECS) == (
            "postgresql",
        )


# --- Core-vocabulary alignment -------------------------------------------------------------
#
# `SupportedDataSources` is the vocabulary Core Expectations declare their support against, and it
# ships inside the package. This registry names the same data sources for its own purposes. The
# check below exists so the two cannot come to name one data source two different ways.
#
# Read from the module-scope snapshot for the same reason as its neighbours: the autouse
# `_snapshot_registry` fixture clears the registry around every test, so a body reading the live
# registry would see an empty one and pass vacuously.
_PUBLIC_NAME_BY_LABEL: Mapping[str, str] = {
    spec.label: spec.public_name for spec in _REGISTERED_DATA_SOURCE_SPECS
}

# Data sources this repository has a record for and the shipped vocabulary has no member for. Held
# as an explicit literal, not derived, because it is what keeps the one-directional check below
# from being a silent ratchet: if a member is added upstream for one of these, the literal stops
# matching and the failure prompts the record to adopt the member's exact value.
_PUBLIC_NAMES_WITH_NO_CORE_MEMBER: Tuple[str, ...] = (
    "Amazon S3",
    "Azure Blob Storage",
    "ClickHouse",
    "Google Cloud Storage",
    "Microsoft Fabric",
    "Oracle",
    "SingleStore",
    "Trino",
)

_CORE_VOCABULARY_MODULE = "great_expectations/expectations/metadata_types.py"


@dataclass(frozen=True)
class _FabricatedMember:
    """A stand-in for a `SupportedDataSources` member, used only to prove the checks can fail.

    The two helpers below take the vocabulary as an argument precisely so a failure can be
    demonstrated without touching `SupportedDataSources` itself, which ships in the package.
    """

    value: str


def _labels_carrying(public_name: str, public_name_by_label: Mapping[str, str]) -> Tuple[str, ...]:
    """Every record whose public name is exactly `public_name`, in label order.

    A tuple rather than a single label: variants of one publicly named data source deliberately
    share a public name (the two pandas records both say `Pandas`), so a member legitimately
    resolves to more than one record. Returning all of them keeps "at least one" honest without
    hiding how many there are.
    """
    return tuple(
        label for label, name in sorted(public_name_by_label.items()) if name == public_name
    )


def _members_reaching_no_record(
    members: Sequence[Any], public_name_by_label: Mapping[str, str]
) -> Tuple[str, ...]:
    """The values of `members` that no record carries as its public name, in declaration order."""
    return tuple(
        member.value
        for member in members
        if not _labels_carrying(member.value, public_name_by_label)
    )


def _public_names_reaching_no_member(
    members: Sequence[Any], public_name_by_label: Mapping[str, str]
) -> Tuple[str, ...]:
    """The record public names that no member of `members` names, sorted and de-duplicated."""
    member_values = {member.value for member in members}
    return tuple(
        sorted({name for name in public_name_by_label.values() if name not in member_values})
    )


def _unreachable_member_message(missing: Sequence[str]) -> str:
    return (
        f"{_CORE_VOCABULARY_MODULE} declares SupportedDataSources members that no registered "
        f"record carries as its public_name: {list(missing)}. Core Expectations claim support for "
        "these data sources, so the registry cannot be silent about them. Remedy, either: set the "
        "public_name of the record for that data source to the member's exact value, or — if this "
        "repository genuinely has no record for it — register one. A member must never be dropped "
        f"from {_CORE_VOCABULARY_MODULE} to make this pass; that file is a shipped public surface."
    )


def _absent_set_drift_message(actual: Sequence[str], expected: Sequence[str]) -> str:
    newly_named = sorted(set(expected) - set(actual))
    newly_absent = sorted(set(actual) - set(expected))
    return (
        "The set of registered public names with no SupportedDataSources member has drifted from "
        f"_PUBLIC_NAMES_WITH_NO_CORE_MEMBER in {Path(__file__).name}. No longer absent — a "
        f"member now names them, or the record was renamed: {newly_named}. Newly absent: "
        f"{newly_absent}. Remedy, either: update the record's "
        f"public_name to the exact value of the member in {_CORE_VOCABULARY_MODULE} that now names "
        "it, or update the _PUBLIC_NAMES_WITH_NO_CORE_MEMBER literal to record the new gap."
    )


class TestCoreVocabularyAlignment:
    """The registry's public names and the shipped `SupportedDataSources` vocabulary agree.

    The check is one-directional on purpose: every *member* must reach a record, but a record need
    not have a member. The reverse direction would fail immediately for eight data sources this
    work declares — the three object stores, Microsoft Fabric, Trino, ClickHouse, Oracle and
    SingleStore — and closing that gap is not this work's to close: `SupportedDataSources` is a
    public metadata surface in the shipped package, so adding a member to it is a product decision
    about what Core Expectations advertise, with user-visible consequences. A test harness does not
    get to force one.

    What stops the one direction from being a silent ratchet is the second assertion below: the
    eight names with no member are pinned as a literal, so a member added upstream for any of them
    fails here and prompts the record to adopt that member's exact value.
    """

    def test_every_core_vocabulary_member_resolves_to_at_least_one_registered_record(self) -> None:
        missing = _members_reaching_no_record(tuple(SupportedDataSources), _PUBLIC_NAME_BY_LABEL)
        assert missing == (), _unreachable_member_message(missing)

    def test_a_member_naming_variants_of_one_data_source_resolves_to_every_variant(self) -> None:
        """`Pandas` is carried by two records by design; "at least one" must not hide the second."""
        assert _labels_carrying(SupportedDataSources.PANDAS.value, _PUBLIC_NAME_BY_LABEL) == (
            "pandas-data-frame",
            "pandas-filesystem-csv",
        )

    def test_the_registered_names_with_no_core_member_equal_the_reviewed_literal(self) -> None:
        actual = _public_names_reaching_no_member(
            tuple(SupportedDataSources), _PUBLIC_NAME_BY_LABEL
        )
        assert actual == _PUBLIC_NAMES_WITH_NO_CORE_MEMBER, _absent_set_drift_message(
            actual, _PUBLIC_NAMES_WITH_NO_CORE_MEMBER
        )

    def test_a_fabricated_member_naming_no_record_is_reported_with_both_remedies(self) -> None:
        """The forward half is able to fail.

        A member the registry has no record for is what an upstream addition looks like from here.
        `SupportedDataSources` itself is never touched: the fabricated member is passed in, which
        is the whole reason the two helpers take the vocabulary as an argument.
        """
        fabricated = (_FabricatedMember("Nowhere DB"),)

        missing = _members_reaching_no_record(fabricated, _PUBLIC_NAME_BY_LABEL)

        assert missing == ("Nowhere DB",)
        message = _unreachable_member_message(missing)
        assert "Nowhere DB" in message
        assert "set the public_name of the record" in message
        assert "register one" in message

    def test_a_member_added_for_a_known_absent_name_breaks_the_literal_with_both_remedies(
        self,
    ) -> None:
        """The known-absent half is able to fail.

        Simulates upstream adding a `SupportedDataSources` member for Trino — one of the eight —
        by passing an extended vocabulary in. Trino must then leave the known-absent set, so the
        literal no longer matches and the message says how to reconcile it.
        """
        extended = tuple(SupportedDataSources) + (_FabricatedMember("Trino"),)

        actual = _public_names_reaching_no_member(extended, _PUBLIC_NAME_BY_LABEL)

        assert actual != _PUBLIC_NAMES_WITH_NO_CORE_MEMBER
        assert "Trino" not in actual
        message = _absent_set_drift_message(actual, _PUBLIC_NAMES_WITH_NO_CORE_MEMBER)
        assert "No longer absent" in message
        assert "['Trino']" in message
        assert "update the record's public_name" in message
        assert "update the _PUBLIC_NAMES_WITH_NO_CORE_MEMBER literal" in message
