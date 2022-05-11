import logging
from copy import deepcopy
from typing import Any, List, Optional, Tuple

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchDefinition,
    BatchMarkers,
    BatchRequestBase,
)
from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class DataConnector:
    '\n    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines\n    can use to get individual batches of data. They add flexibility in how to obtain data\n    such as with time-based partitioning, downsampling, or other techniques appropriate\n    for the Datasource.\n\n    For example, a DataConnector could produce a SQL query that logically represents "rows in\n    the Events table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource\n    could use to materialize a SqlAlchemyDataset corresponding to that batch of data and\n    ready for validation.\n\n    A batch is a sample from a data asset, sliced according to a particular rule. For\n    example, an hourly slide of the Events table or “most recent `users` records.”\n\n    A Batch is the primary unit of validation in the Great Expectations DataContext.\n    Batches include metadata that identifies how they were constructed--the same “batch_spec”\n    assembled by the data connector, While not every Datasource will enable re-fetching a\n    specific batch of data, GE can store snapshots of batches or store metadata from an\n    external data version control system.\n'

    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: ExecutionEngine,
        batch_spec_passthrough: Optional[dict] = None,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Base class for DataConnectors\n\n        Args:\n            name (str): required name for DataConnector\n            datasource_name (str): required name for datasource\n            execution_engine (ExecutionEngine): reference to ExecutionEngine\n            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec\n        "
        if execution_engine is None:
            raise ge_exceptions.DataConnectorError(
                "A non-existent/unknown ExecutionEngine instance was referenced."
            )
        self._name = name
        self._datasource_name = datasource_name
        self._execution_engine = execution_engine
        self._data_references_cache = {}
        self._data_context_root_directory = None
        self._batch_spec_passthrough = batch_spec_passthrough or {}

    @property
    def batch_spec_passthrough(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._batch_spec_passthrough

    @property
    def name(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._name

    @property
    def datasource_name(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._datasource_name

    @property
    def execution_engine(self) -> ExecutionEngine:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._execution_engine

    @property
    def data_context_root_directory(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._data_context_root_directory

    @data_context_root_directory.setter
    def data_context_root_directory(self, data_context_root_directory: str) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._data_context_root_directory = data_context_root_directory

    def get_batch_data_and_metadata(
        self, batch_definition: BatchDefinition
    ) -> Tuple[(Any, BatchSpec, BatchMarkers)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Uses batch_definition to retrieve batch_data and batch_markers by building a batch_spec from batch_definition,\n        then using execution_engine to return batch_data and batch_markers\n\n        Args:\n            batch_definition (BatchDefinition): required batch_definition parameter for retrieval\n\n        "
        batch_spec: BatchSpec = self.build_batch_spec(batch_definition=batch_definition)
        (batch_data, batch_markers) = self._execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )
        self._execution_engine.load_batch_data(batch_definition.id, batch_data)
        return (batch_data, batch_spec, batch_markers)

    def build_batch_spec(self, batch_definition: BatchDefinition) -> BatchSpec:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Builds batch_spec from batch_definition by generating batch_spec params and adding any pass_through params\n\n        Args:\n            batch_definition (BatchDefinition): required batch_definition parameter for retrieval\n        Returns:\n            BatchSpec object built from BatchDefinition\n\n        "
        batch_spec_params: dict = (
            self._generate_batch_spec_parameters_from_batch_definition(
                batch_definition=batch_definition
            )
        )
        batch_spec_passthrough: dict = deepcopy(self.batch_spec_passthrough)
        if isinstance(batch_definition.batch_spec_passthrough, dict):
            batch_spec_passthrough.update(batch_definition.batch_spec_passthrough)
        batch_spec_params.update(batch_spec_passthrough)
        batch_spec: BatchSpec = BatchSpec(**batch_spec_params)
        return batch_spec

    def _refresh_data_references_cache(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        List objects in the underlying data store to create a list of data_references.\n        This method is used to refresh the cache by classes that extend this base DataConnector class\n\n        Args:\n            data_asset_name (str): optional data_asset_name to retrieve more specific results\n\n        "
        raise NotImplementedError

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[Any]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Fetch data_references corresponding to data_asset_name from the cache.\n        "
        raise NotImplementedError

    def get_data_reference_list_count(self) -> int:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def get_unmatched_data_references(self) -> List[Any]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def get_available_data_asset_names(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Return the list of asset names known by this data connector.\n\n        Returns:\n            A list of available names\n        "
        raise NotImplementedError

    def get_available_data_asset_names_and_types(self) -> List[Tuple[(str, str)]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Return the list of asset names and types known by this DataConnector.\n\n        Returns:\n            A list of tuples consisting of available names and types\n        "
        raise NotImplementedError

    def get_batch_definition_list_from_batch_request(
        self, batch_request: BatchRequestBase
    ) -> List[BatchDefinition]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def _map_data_reference_to_batch_definition_list(
        self, data_reference: Any, data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def _map_batch_definition_to_data_reference(
        self, batch_definition: BatchDefinition
    ) -> Any:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def self_check(self, pretty_print=True, max_examples=3):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Checks the configuration of the current DataConnector by doing the following :\n\n        1. refresh or create data_reference_cache\n        2. print batch_definition_count and example_data_references for each data_asset_names\n        3. also print unmatched data_references, and allow the user to modify the regex or glob configuration if necessary\n        4. select a random data_reference and attempt to retrieve and print the first few rows to user\n\n        When used as part of the test_yaml_config() workflow, the user will be able to know if the data_connector is properly configured,\n        and if the associated execution_engine can properly retrieve data using the configuration.\n\n        Args:\n            pretty_print (bool): should the output be printed?\n            max_examples (int): how many data_references should be printed?\n\n        Returns:\n            report_obj (dict): dictionary containing self_check output\n\n        "
        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()
        if pretty_print:
            print(f"	{self.name}", ":", self.__class__.__name__)
            print()
        asset_names = self.get_available_data_asset_names()
        asset_names.sort()
        len_asset_names = len(asset_names)
        report_obj = {
            "class_name": self.__class__.__name__,
            "data_asset_count": len_asset_names,
            "example_data_asset_names": asset_names[:max_examples],
            "data_assets": {},
        }
        if pretty_print:
            print(
                f"	Available data_asset_names ({min(len_asset_names, max_examples)} of {len_asset_names}):"
            )
        for asset_name in asset_names[:max_examples]:
            data_reference_list = (
                self._get_data_reference_list_from_cache_by_data_asset_name(
                    data_asset_name=asset_name
                )
            )
            len_batch_definition_list = len(data_reference_list)
            example_data_references = data_reference_list[:max_examples]
            if pretty_print:
                print(
                    f"		{asset_name} ({min(len_batch_definition_list, max_examples)} of {len_batch_definition_list}):",
                    example_data_references,
                )
            report_obj["data_assets"][asset_name] = {
                "batch_definition_count": len_batch_definition_list,
                "example_data_references": example_data_references,
            }
        unmatched_data_references = self.get_unmatched_data_references()
        len_unmatched_data_references = len(unmatched_data_references)
        if pretty_print:
            print(
                f"""
	Unmatched data_references ({min(len_unmatched_data_references, max_examples)} of {len_unmatched_data_references}):{unmatched_data_references[:max_examples]}
"""
            )
        report_obj["unmatched_data_reference_count"] = len_unmatched_data_references
        report_obj["example_unmatched_data_references"] = unmatched_data_references[
            :max_examples
        ]
        return report_obj

    def _self_check_fetch_batch(
        self, pretty_print: bool, example_data_reference: Any, data_asset_name: str
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Helper function for self_check() to retrieve batch using example_data_reference and data_asset_name,\n        all while printing helpful messages. First 5 rows of batch_data are printed by default.\n\n        Args:\n            pretty_print (bool): print to console?\n            example_data_reference (Any): data_reference to retrieve\n            data_asset_name (str): data_asset_name to retrieve\n\n        "
        if pretty_print:
            print("\n\t\tFetching batch data...")
        batch_definition_list: List[
            BatchDefinition
        ] = self._map_data_reference_to_batch_definition_list(
            data_reference=example_data_reference, data_asset_name=data_asset_name
        )
        assert len(batch_definition_list) == 1
        batch_definition: BatchDefinition = batch_definition_list[0]
        if batch_definition is None:
            return {}
        batch_data: Any
        batch_spec: BatchSpec
        (batch_data, batch_spec, _) = self.get_batch_data_and_metadata(
            batch_definition=batch_definition
        )
        validator: Validator = Validator(execution_engine=batch_data.execution_engine)
        data: Any = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.head",
                metric_domain_kwargs={"batch_id": batch_definition.id},
                metric_value_kwargs={"n_rows": 5},
            )
        )
        n_rows: int = validator.get_metric(
            metric=MetricConfiguration(
                metric_name="table.row_count",
                metric_domain_kwargs={"batch_id": batch_definition.id},
            )
        )
        if pretty_print and (data is not None):
            print("\n\t\tShowing 5 rows")
            print(data)
        return {"batch_spec": batch_spec, "n_rows": n_rows}

    def _validate_batch_request(self, batch_request: BatchRequestBase) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Validate batch_request by checking:\n            1. if configured datasource_name matches batch_request's datasource_name\n            2. if current data_connector_name matches batch_request's data_connector_name\n        Args:\n            batch_request (BatchRequestBase): batch_request object to validate\n\n        "
        if batch_request.datasource_name != self.datasource_name:
            raise ValueError(
                f'datasource_name in BatchRequest: "{batch_request.datasource_name}" does not match DataConnector datasource_name: "{self.datasource_name}".'
            )
        if batch_request.data_connector_name != self.name:
            raise ValueError(
                f'data_connector_name in BatchRequest: "{batch_request.data_connector_name}" does not match DataConnector name: "{self.name}".'
            )
