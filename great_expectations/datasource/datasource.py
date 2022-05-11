import copy
import logging
import warnings

from great_expectations.data_context.util import (
    instantiate_class_from_config,
    load_class,
    verify_dynamic_loading_support,
)
from great_expectations.exceptions import ClassInstantiationError

logger = logging.getLogger(__name__)


class LegacyDatasource:
    '\n    A Datasource connects to a compute environment and one or more storage environments and produces batches of data\n    that Great Expectations can validate in that compute environment.\n\n    Each Datasource provides Batches connected to a specific compute environment, such as a\n    SQL database, a Spark cluster, or a local in-memory Pandas DataFrame.\n\n    Datasources use Batch Kwargs to specify instructions for how to access data from relevant sources such as an\n    existing object from a DAG runner, a SQL database, S3 bucket, or local filesystem.\n\n    To bridge the gap between those worlds, Datasources interact closely with *generators* which\n    are aware of a source of data and can produce produce identifying information, called\n    "batch_kwargs" that datasources can use to get individual batches of data. They add flexibility\n    in how to obtain data such as with time-based partitioning, downsampling, or other techniques\n    appropriate for the datasource.\n\n    For example, a batch kwargs generator could produce a SQL query that logically represents "rows in the Events\n    table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource could use to materialize\n    a SqlAlchemyDataset corresponding to that batch of data and ready for validation.\n\n    Since opinionated DAG managers such as airflow, dbt, prefect.io, dagster can also act as datasources\n    and/or batch kwargs generators for a more generic datasource.\n\n    When adding custom expectations by subclassing an existing DataAsset type, use the data_asset_type parameter\n    to configure the datasource to load and return DataAssets of the custom type.\n\n    --ge-feature-maturity-info--\n\n        id: datasource_s3\n        title: Datasource - S3\n        icon:\n        short_description: S3\n        description: Support for connecting to Amazon Web Services S3 as an external datasource.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_datasources/how_to_configure_a_pandas_s3_datasource.html\n        maturity: Production\n        maturity_details:\n            api_stability: medium\n            implementation_completeness: Complete\n            unit_test_coverage:: Complete\n            integration_infrastructure_test_coverage: None\n            documentation_completeness: Minimal/Spotty\n            bug_risk: Low\n\n        id: datasource_filesystem\n        title: Datasource - Filesystem\n        icon:\n        short_description: File-based datsource\n        description: Support for using a mounted filesystem as an external datasource.\n        how_to_guide_url: https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_datasources/how_to_configure_a_pandas_filesystem_datasource.html\n        maturity: Production\n        maturity_details:\n            api_stability: Medium\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: Partial\n            documentation_completeness: Partial\n            bug_risk: Low (Moderate for Windows users because of path issues)\n\n        id: datasource_gcs\n        title: Datasource - GCS\n        icon:\n        short_description: GCS\n        description: Support for Google Cloud Storage as an external datasource\n        how_to_guide_url:\n        maturity: Experimental\n        maturity_details:\n            api_stability: Medium (supported via native ‘gs://\' syntax in Pandas and Pyspark; medium because we expect configuration to evolve)\n            implementation_completeness: Medium (works via passthrough, not via CLI)\n            unit_test_coverage: Minimal\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Minimal\n            bug_risk: Moderate\n\n        id: datasource_azure_blob_storage\n        title: Datasource - Azure Blob Storage\n        icon:\n        short_description: Azure Blob Storage\n        description: Support for Microsoft Azure Blob Storage as an external datasource\n        how_to_guide_url:\n        maturity: In Roadmap (Sub-Experimental - "Not Impossible")\n        maturity_details:\n            api_stability: N/A (Supported on Databricks Spark via ‘wasb://\' / ‘wasps://\' url; requires local download first for Pandas)\n            implementation_completeness: Minimal\n            unit_test_coverage: N/A\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Minimal\n            bug_risk: Unknown\n    --ge-feature-maturity-info--\n'
    recognized_batch_parameters = {"limit"}

    @classmethod
    def from_configuration(cls, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Build a new datasource from a configuration dictionary.\n\n        Args:\n            **kwargs: configuration key-value pairs\n\n        Returns:\n            datasource (Datasource): the newly-created datasource\n\n        "
        return cls(**kwargs)

    @classmethod
    def build_configuration(
        cls,
        class_name,
        module_name="great_expectations.datasource",
        data_asset_type=None,
        batch_kwargs_generators=None,
        **kwargs,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Build a full configuration object for a datasource, potentially including batch kwargs generators with defaults.\n\n        Args:\n            class_name: The name of the class for which to build the config\n            module_name: The name of the module in which the datasource class is located\n            data_asset_type: A ClassConfig dictionary\n            batch_kwargs_generators: BatchKwargGenerators configuration dictionary\n            **kwargs: Additional kwargs to be part of the datasource constructor's initialization\n\n        Returns:\n            A complete datasource configuration.\n\n        "
        verify_dynamic_loading_support(module_name=module_name)
        class_ = load_class(class_name=class_name, module_name=module_name)
        configuration = class_.build_configuration(
            data_asset_type=data_asset_type,
            batch_kwargs_generators=batch_kwargs_generators,
            **kwargs,
        )
        return configuration

    def __init__(
        self,
        name,
        data_context=None,
        data_asset_type=None,
        batch_kwargs_generators=None,
        **kwargs,
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Build a new datasource.\n\n        Args:\n            name: the name for the datasource\n            data_context: data context to which to connect\n            data_asset_type (ClassConfig): the type of DataAsset to produce\n            batch_kwargs_generators: BatchKwargGenerators to add to the datasource\n        "
        self._data_context = data_context
        self._name = name
        if isinstance(data_asset_type, str):
            warnings.warn(
                "String-only configuration for data_asset_type is deprecated as of v0.7.11. As support will be removed in v0.16, please use module_name and class_name instead.",
                DeprecationWarning,
            )
        self._data_asset_type = data_asset_type
        self._datasource_config = kwargs
        self._batch_kwargs_generators = {}
        self._datasource_config["data_asset_type"] = data_asset_type
        if batch_kwargs_generators is not None:
            self._datasource_config["batch_kwargs_generators"] = batch_kwargs_generators

    @property
    def name(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Property for datasource name\n        "
        return self._name

    @property
    def config(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return copy.deepcopy(self._datasource_config)

    @property
    def data_context(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Property for attached DataContext\n        "
        return self._data_context

    def _build_generators(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Build batch kwargs generator objects from the datasource configuration.\n\n        Returns:\n            None\n        "
        try:
            for generator in self._datasource_config["batch_kwargs_generators"].keys():
                self.get_batch_kwargs_generator(generator)
        except KeyError:
            pass

    def add_batch_kwargs_generator(self, name, class_name, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Add a BatchKwargGenerator to the datasource.\n\n        Args:\n            name (str): the name of the new BatchKwargGenerator to add\n            class_name: class of the BatchKwargGenerator to add\n            kwargs: additional keyword arguments will be passed directly to the new BatchKwargGenerator's constructor\n\n        Returns:\n             BatchKwargGenerator (BatchKwargGenerator)\n        "
        kwargs["class_name"] = class_name
        generator = self._build_batch_kwargs_generator(**kwargs)
        if "batch_kwargs_generators" not in self._datasource_config:
            self._datasource_config["batch_kwargs_generators"] = {}
        self._datasource_config["batch_kwargs_generators"][name] = kwargs
        return generator

    def _build_batch_kwargs_generator(self, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Build a BatchKwargGenerator using the provided configuration and return the newly-built generator."
        generator = instantiate_class_from_config(
            config=kwargs,
            runtime_environment={"datasource": self},
            config_defaults={
                "module_name": "great_expectations.datasource.batch_kwargs_generator"
            },
        )
        if not generator:
            raise ClassInstantiationError(
                module_name="great_expectations.datasource.batch_kwargs_generator",
                package_name=None,
                class_name=kwargs["class_name"],
            )
        return generator

    def get_batch_kwargs_generator(self, name):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get the (named) BatchKwargGenerator from a datasource\n\n        Args:\n            name (str): name of BatchKwargGenerator (default value is 'default')\n\n        Returns:\n            BatchKwargGenerator (BatchKwargGenerator)\n        "
        if name in self._batch_kwargs_generators:
            return self._batch_kwargs_generators[name]
        elif ("batch_kwargs_generators" in self._datasource_config) and (
            name in self._datasource_config["batch_kwargs_generators"]
        ):
            generator_config = copy.deepcopy(
                self._datasource_config["batch_kwargs_generators"][name]
            )
        else:
            raise ValueError(
                "Unable to load batch kwargs generator %s -- no configuration found or invalid configuration."
                % name
            )
        generator = self._build_batch_kwargs_generator(**generator_config)
        self._batch_kwargs_generators[name] = generator
        return generator

    def list_batch_kwargs_generators(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        'List currently-configured BatchKwargGenerator for this datasource.\n\n        Returns:\n            List(dict): each dictionary includes "name" and "type" keys\n        '
        generators = []
        if "batch_kwargs_generators" in self._datasource_config:
            for (key, value) in self._datasource_config[
                "batch_kwargs_generators"
            ].items():
                generators.append({"name": key, "class_name": value["class_name"]})
        return generators

    def process_batch_parameters(self, limit=None, dataset_options=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Use datasource-specific configuration to translate any batch parameters into batch kwargs at the datasource\n        level.\n\n        Args:\n            limit (int): a parameter all datasources must accept to allow limiting a batch to a smaller number of rows.\n            dataset_options (dict): a set of kwargs that will be passed to the constructor of a dataset built using\n                these batch_kwargs\n\n        Returns:\n            batch_kwargs: Result will include both parameters passed via argument and configured parameters.\n        "
        batch_kwargs = self._datasource_config.get("batch_kwargs", {})
        if limit is not None:
            batch_kwargs["limit"] = limit
        if dataset_options is not None:
            if not batch_kwargs.get("dataset_options"):
                batch_kwargs["dataset_options"] = {}
            batch_kwargs["dataset_options"].update(dataset_options)
        return batch_kwargs

    def get_batch(self, batch_kwargs, batch_parameters=None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get a batch of data from the datasource.\n\n        Args:\n            batch_kwargs: the BatchKwargs to use to construct the batch\n            batch_parameters: optional parameters to store as the reference description of the batch. They should\n                reflect parameters that would provide the passed BatchKwargs.\n\n\n        Returns:\n            Batch\n\n        "
        raise NotImplementedError

    def get_available_data_asset_names(self, batch_kwargs_generator_names=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Returns a dictionary of data_asset_names that the specified batch kwarg\n        generator can provide. Note that some batch kwargs generators may not be\n        capable of describing specific named data assets, and some (such as\n        filesystem glob batch kwargs generators) require the user to configure\n        data asset names.\n\n        Args:\n            batch_kwargs_generator_names: the BatchKwargGenerator for which to get available data asset names.\n\n        Returns:\n            dictionary consisting of sets of generator assets available for the specified generators:\n            ::\n\n                {\n                  generator_name: {\n                    names: [ (data_asset_1, data_asset_1_type), (data_asset_2, data_asset_2_type) ... ]\n                  }\n                  ...\n                }\n\n        "
        available_data_asset_names = {}
        if batch_kwargs_generator_names is None:
            batch_kwargs_generator_names = [
                generator["name"] for generator in self.list_batch_kwargs_generators()
            ]
        elif isinstance(batch_kwargs_generator_names, str):
            batch_kwargs_generator_names = [batch_kwargs_generator_names]
        for generator_name in batch_kwargs_generator_names:
            generator = self.get_batch_kwargs_generator(generator_name)
            available_data_asset_names[
                generator_name
            ] = generator.get_available_data_asset_names()
        return available_data_asset_names

    def build_batch_kwargs(
        self, batch_kwargs_generator, data_asset_name=None, partition_id=None, **kwargs
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if kwargs.get("name"):
            if data_asset_name:
                raise ValueError(
                    "Cannot provide both 'name' and 'data_asset_name'. Please use 'data_asset_name' only."
                )
            warnings.warn(
                "name is deprecated as a batch_parameter as of v0.11.2 and will be removed in v0.16. Please use data_asset_name instead.",
                DeprecationWarning,
            )
            data_asset_name = kwargs.pop("name")
        generator_obj = self.get_batch_kwargs_generator(batch_kwargs_generator)
        if partition_id is not None:
            kwargs["partition_id"] = partition_id
        return generator_obj.build_batch_kwargs(
            data_asset_name=data_asset_name, **kwargs
        )
