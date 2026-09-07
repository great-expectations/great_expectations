import pathlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Optional, Union

import pandas as pd
import pytest

from great_expectations.compatibility.pyspark import types as pyspark_types
from great_expectations.compatibility.typing_extensions import override
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import CSVAsset
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.execution_engine import SparkDFExecutionEngine
from tests.integration.sql_session_manager import SessionSQLEngineManager
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
from tests.integration.test_utils.data_source_config.registry import register_data_source_config

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark


@register_data_source_config
@dataclass(frozen=True)
class SparkFilesystemCsvDatasourceTestConfig(DataSourceTestConfig):
    DATA_SOURCE_SPEC = DataSourceSpec(
        label="spark-filesystem-csv",
        # The name the shipped supported-data-source vocabulary already fixes for this data
        # source; spelling a second one here is exactly the drift this record exists to remove.
        public_name="Spark",
        # LOCAL_FILE, not LOCAL_CONTAINER, and the distinction is easy to get backwards here.
        # A compose directory does exist for this marker and the task runner's entry for it does
        # name that service, so declaring a container would pass the wiring drift check - and
        # still say something untrue. That compose file starts a Spark Connect server, and exists
        # so a host without a modern JDK can run Spark tests against it; this config starts no
        # server at all. It builds an in-process Spark session and reads CSV files off the local
        # filesystem, so a test run obtains its instance the same way SQLite does. Recorded here
        # because a later reader will find the compose file and wonder why it is not named.
        provisioning=DataSourceProvisioning.LOCAL_FILE,
        execution_engine=ExecutionEngineKind.SPARK,
        fluent_types=frozenset({"spark_filesystem"}),
        marker="spark",
        # Shared, not dedicated: `spark` selects everything Spark-dependent, which is a class of
        # tests rather than this config's tests alone. More than one record may legitimately
        # declare it, so it must not be checked for collision as though it named this data
        # source by itself.
        marker_scope=MarkerScope.SHARED,
        ci_lane=CiLaneRef(workflow_job="marker-tests", marker_token="spark"),
        # The shared canonical expectation parameterization runs against this config in the
        # `spark` lane today, through every one of its expectation modules. Declaring the tier
        # states that existing result; it switches nothing on. The marker and CI lane the claim
        # obliges are already declared above.
        tiers=frozenset({SupportTier.CANONICAL_EXPECTATIONS, SupportTier.FLUENT_API}),
        dev_requirements_file="reqs/requirements-dev-spark.txt",
        task_runner_marker="spark",
    )

    # see "read" options: https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html#data-source-option
    read_options: dict[str, Any] = field(default_factory=dict)
    # see "write" options: https://spark.apache.org/docs/3.5.3/sql-data-sources-csv.html#data-source-option
    write_options: dict[str, Any] = field(default_factory=dict)

    @override
    def create_batch_setup(
        self,
        request: pytest.FixtureRequest,
        data: pd.DataFrame,
        extra_data: Mapping[str, pd.DataFrame],
        context: AbstractDataContext,
        engine_manager: Optional[SessionSQLEngineManager] = None,
    ) -> BatchTestSetup:
        assert not extra_data, "extra_data is not supported for this data source yet."

        tmp_path = request.getfixturevalue("tmp_path")
        assert isinstance(tmp_path, pathlib.Path)

        return SparkFilesystemCsvBatchTestSetup(
            data=data,
            config=self,
            base_dir=tmp_path,
            context=context,
        )


class SparkFilesystemCsvBatchTestSetup(
    BatchTestSetup[SparkFilesystemCsvDatasourceTestConfig, CSVAsset]
):
    def __init__(
        self,
        config: SparkFilesystemCsvDatasourceTestConfig,
        data: pd.DataFrame,
        base_dir: pathlib.Path,
        context: AbstractDataContext,
    ) -> None:
        super().__init__(config=config, data=data, context=context)
        self._base_dir = base_dir

    @property
    def _spark_session(self) -> "pyspark.SparkSession":
        return SparkDFExecutionEngine.get_or_create_spark_session()

    @property
    def _spark_schema(self) -> Union["pyspark_types.StructType", None]:
        column_types = self.config.column_types or {}
        struct_fields = [
            pyspark_types.StructField(column_name, column_type())
            for column_name, column_type in column_types.items()
        ]
        return pyspark_types.StructType(struct_fields) if struct_fields else None

    @property
    def _spark_data(self) -> "pyspark.DataFrame":
        from pyspark.sql.types import _acceptable_types as spark_acceptable_types

        # Pandas 3's pd.NA and StringDtype inference break PySpark's
        # createDataFrame, so we extract plain-Python rows via itertuples,
        # replacing NA → None and coercing to PySpark-compatible types.
        schema = self._spark_schema

        # Per-column (target_type, acceptable_types) from PySpark's internal
        # _acceptable_types dict, e.g. IntegerType → (int, (int,)).
        # PySpark checks exact types (not isinstance), so we do the same.
        _TypeInfo = tuple[type, tuple[type, ...]]
        type_info_for_col: dict[str, _TypeInfo] = {
            f.name: (accepted[0], accepted)
            for f in (schema or [])
            if (accepted := spark_acceptable_types.get(type(f.dataType)))
        }
        column_type_info: tuple[_TypeInfo | None, ...] = tuple(
            type_info_for_col.get(c) for c in self.data.columns
        )

        def _clean(val: object, info: _TypeInfo | None) -> object:
            if pd.isna(val):  # type: ignore[call-overload]  # scalar from itertuples
                return None
            # PySpark matches on exact type, and `pd.Timestamp` is a subclass rather than the
            # type it looks for. Unconditional, because this is needed most where no schema was
            # declared: with one, the value is checked against a known field type; without one,
            # PySpark infers from the value alone and reads a timestamp it does not recognise as
            # an empty struct, which is not a column any file format can be asked to write.
            if isinstance(val, pd.Timestamp):
                return val.to_pydatetime()
            if info is not None:
                target, acceptable = info
                if type(val) not in acceptable:
                    return target(val)
            return val

        rows = [
            tuple(_clean(val, info) for val, info in zip(record, column_type_info, strict=True))
            for record in self.data.itertuples(index=False, name=None)
        ]

        if schema:
            return self._spark_session.createDataFrame(rows, schema=schema)
        return self._spark_session.createDataFrame(rows, list(self.data.columns))

    @override
    def make_asset(self) -> CSVAsset:
        infer_schema = self._spark_schema is None
        return self.context.data_sources.add_spark_filesystem(
            name=self._random_resource_name(), base_directory=self._base_dir
        ).add_csv_asset(
            name=self._random_resource_name(),
            spark_schema=self._spark_schema,
            header=True,
            infer_schema=infer_schema,
            **self.config.read_options,
        )

    @override
    def make_batch(self) -> Batch:
        return (
            self.make_asset()
            .add_batch_definition_path(name=self._random_resource_name(), path=self.csv_path)
            .get_batch()
        )

    @override
    def setup(self) -> None:
        file_path = self._base_dir / self.csv_path
        self._spark_data.write.format("csv").option("header", True).options(
            **self.config.write_options
        ).save(str(file_path))

    @override
    def teardown(self) -> None: ...

    @property
    def csv_path(self) -> pathlib.Path:
        return pathlib.Path("data.csv")
