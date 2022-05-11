import logging
from typing import List, Optional

try:
    import boto3
except ImportError:
    boto3 = None
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import PathBatchSpec, S3BatchSpec
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.configured_asset_file_path_data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.util import list_s3_keys
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class ConfiguredAssetS3DataConnector(ConfiguredAssetFilePathDataConnector):
    '\n    Extension of ConfiguredAssetFilePathDataConnector used to connect to S3\n\n    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines\n    can use to get individual batches of data. They add flexibility in how to obtain data\n    such as with time-based partitioning, downsampling, or other techniques appropriate\n    for the Datasource.\n\n    The ConfiguredAssetS3DataConnector is one of two classes (InferredAssetS3DataConnector being the\n    other one) designed for connecting to data on S3.\n\n    A ConfiguredAssetS3DataConnector requires an explicit listing of each DataAsset you want to connect to.\n    This allows more fine-tuning, but also requires more setup.\n'

    def __init__(
        self,
        name: str,
        datasource_name: str,
        bucket: str,
        assets: dict,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        boto3_options: Optional[dict] = None,
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
        "\n        ConfiguredAssetDataConnector for connecting to S3.\n\n        Args:\n            name (str): required name for DataConnector\n            datasource_name (str): required name for datasource\n            bucket (str): bucket for S3\n            assets (dict): dict of asset configuration (required for ConfiguredAssetDataConnector)\n            execution_engine (ExecutionEngine): optional reference to ExecutionEngine\n            default_regex (dict): optional regex configuration for filtering data_references\n            sorters (list): optional list of sorters for sorting data_references\n            prefix (str): S3 prefix\n            delimiter (str): S3 delimiter\n            max_keys (int): S3 max_keys (default is 1000)\n            boto3_options (dict): optional boto3 options\n            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec\n        "
        logger.debug(f'Constructing ConfiguredAssetS3DataConnector "{name}".')
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )
        self._bucket = bucket
        self._prefix = self.sanitize_prefix_for_s3(prefix)
        self._delimiter = delimiter
        self._max_keys = max_keys
        if boto3_options is None:
            boto3_options = {}
        try:
            self._s3 = boto3.client("s3", **boto3_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load boto3 (it is required for ConfiguredAssetS3DataConnector)."
            )

    @staticmethod
    def sanitize_prefix_for_s3(text: str) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Takes in a given user-prefix and cleans it to work with file-system traversal methods\n        (i.e. add '/' to the end of a string meant to represent a directory)\n\n        Customized for S3 paths, ignoring the path separator used by the host OS\n        "
        text = text.strip()
        if not text:
            return text
        path_parts = text.split("/")
        if not path_parts:
            return text
        elif "." in path_parts[(-1)]:
            return text
        else:
            return f"{text.rstrip('/')}/"

    def build_batch_spec(self, batch_definition: BatchDefinition) -> S3BatchSpec:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.\n\n        Args:\n            batch_definition (BatchDefinition): to be used to build batch_spec\n\n        Returns:\n            BatchSpec built from batch_definition\n        "
        batch_spec: PathBatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return S3BatchSpec(batch_spec)

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        query_options: dict = {
            "Bucket": self._bucket,
            "Prefix": self._prefix,
            "Delimiter": self._delimiter,
            "MaxKeys": self._max_keys,
        }
        if asset is not None:
            if asset.bucket:
                query_options["Bucket"] = asset.bucket
            if asset.prefix:
                query_options["Prefix"] = asset.prefix
            if asset.delimiter:
                query_options["Delimiter"] = asset.delimiter
            if asset.max_keys:
                query_options["MaxKeys"] = asset.max_keys
        path_list: List[str] = [
            key
            for key in list_s3_keys(
                s3=self._s3,
                query_options=query_options,
                iterator_dict={},
                recursive=False,
            )
        ]
        return path_list

    def _get_full_file_path_for_asset(
        self, path: str, asset: Optional[Asset] = None
    ) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        template_arguments: dict = {"bucket": self._bucket, "path": path}
        return self.execution_engine.resolve_data_reference(
            data_connector_name=self.__class__.__name__,
            template_arguments=template_arguments,
        )
