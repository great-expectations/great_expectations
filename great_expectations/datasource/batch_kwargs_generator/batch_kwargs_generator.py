import logging
import warnings

from great_expectations.core.id_dict import BatchKwargs

logger = logging.getLogger(__name__)


class BatchKwargsGenerator:
    '\n        BatchKwargsGenerators produce identifying information, called "batch_kwargs" that datasources\n        can use to get individual batches of data. They add flexibility in how to obtain data\n        such as with time-based partitioning, downsampling, or other techniques appropriate\n        for the datasource.\n\n        For example, a batch kwargs generator could produce a SQL query that logically represents "rows in\n        the Events table with a timestamp on February 7, 2012," which a SqlAlchemyDatasource\n        could use to materialize a SqlAlchemyDataset corresponding to that batch of data and\n        ready for validation.\n\n        A batch is a sample from a data asset, sliced according to a particular rule. For\n        example, an hourly slide of the Events table or “most recent `users` records.”\n\n        A Batch is the primary unit of validation in the Great Expectations DataContext.\n        Batches include metadata that identifies how they were constructed--the same “batch_kwargs”\n        assembled by the batch kwargs generator, While not every datasource will enable re-fetching a\n        specific batch of data, GE can store snapshots of batches or store metadata from an\n        external data version control system.\n\n        Example Generator Configurations follow::\n\n            my_datasource_1:\n              class_name: PandasDatasource\n              batch_kwargs_generators:\n                # This generator will provide two data assets, corresponding to the globs defined under the "file_logs"\n                # and "data_asset_2" keys. The file_logs asset will be partitioned according to the match group\n                # defined in partition_regex\n                default:\n                  class_name: GlobReaderBatchKwargsGenerator\n                  base_directory: /var/logs\n                  reader_options:\n                    sep: "\n                  globs:\n                    file_logs:\n                      glob: logs/*.gz\n                      partition_regex: logs/file_(\\d{0,4})_\\.log\\.gz\n                    data_asset_2:\n                      glob: data/*.csv\n\n            my_datasource_2:\n              class_name: PandasDatasource\n              batch_kwargs_generators:\n                # This generator will create one data asset per subdirectory in /data\n                # Each asset will have partitions corresponding to the filenames in that subdirectory\n                default:\n                  class_name: SubdirReaderBatchKwargsGenerator\n                  reader_options:\n                    sep: "\n                  base_directory: /data\n\n            my_datasource_3:\n              class_name: SqlalchemyDatasource\n              batch_kwargs_generators:\n                # This generator will search for a file named with the name of the requested data asset and the\n                # .sql suffix to open with a query to use to generate data\n                 default:\n                    class_name: QueryBatchKwargsGenerator\n\n    --ge-feature-maturity-info--\n\n        id: batch_kwargs_generator_manual\n        title: Batch Kwargs Generator - Manual\n        icon:\n        short_description: Manually configure how files on a filesystem are presented as batches of data\n        description: Manually configure how files on a filesystem are presented as batches of data\n        how_to_guide_url:\n        maturity: Beta\n        maturity_details:\n            api_stability: Mostly Stable (key generator functionality will remain but batch API changes still possible)\n            implementation_completeness: Complete\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Minimal\n            bug_risk: Moderate\n\n        id: batch_kwargs_generator_s3\n        title: Batch Kwargs Generator - S3\n        icon:\n        short_description: Present files on S3 as batches of data\n        description: Present files on S3 as batches of data for profiling and validation\n        how_to_guide_url:\n        maturity: Beta\n        maturity_details:\n            api_stability: Mostly Stable (expect changes in partitioning)\n            implementation_completeness: Partial\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: Complete\n            documentation_completeness: Minimal\n            bug_risk: Moderate\n\n        id: batch_kwargs_generator_glob_reader\n        title: Batch Kwargs Generator - Glob Reader\n        icon:\n        short_description: A configurable way to present files in a directory as batches of data\n        description: A configurable way to present files in a directory as batches of data\n        how_to_guide_url:\n        maturity: Beta\n        maturity_details:\n            api_stability: Mostly Stable (expect changes in partitioning)\n            implementation_completeness: Partial\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Minimal\n            bug_risk: Moderate\n\n        id: batch_kwargs_generator_table\n        title: Batch Kwargs Generator - Table\n        icon:\n        short_description: Present database tables as batches of data\n        description: Present database tables as batches of data for validation and profiling\n        how_to_guide_url:\n        maturity: Beta\n        maturity_details:\n            api_stability: Unstable (no existing native support for "partitioning")\n            implementation_completeness: Minimal\n            unit_test_coverage: Partial\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Partial\n            bug_risk: Low\n\n        id: batch_kwargs_generator_query\n        title: Batch Kwargs Generator - Query\n        icon:\n        short_description: Present the result sets of SQL queries as batches of data\n        description: Present the result sets of SQL queries as batches of data for validation and profiling\n        how_to_guide_url:\n        maturity: Beta\n        maturity_details:\n            api_stability: Unstable (expect changes in query template configuration and query storage)\n            implementation_completeness: Complete\n            unit_test_coverage: Partial\n            integration_infrastructure_test_coverage: Minimal\n            documentation_completeness: Partial\n            bug_risk: Moderate\n\n        id: batch_kwargs_generator_subdir_reader\n        title: Batch Kwargs Generator - Subdir Reader\n        icon:\n        short_description: Present the files in a directory as batches of data\n        description: Present the files in a directory as batches of data for profiling and validation.\n        how_to_guide_url:\n        maturity: Beta\n        maturity_details:\n            api_stability: Mostly Stable (new configuration options likely)\n            implementation_completeness: Partial\n            unit_test_coverage: Complete\n            integration_infrastructure_test_coverage: N/A\n            documentation_completeness: Minimal\n            bug_risk: Low\n\n    --ge-feature-maturity-info--\n'
    _batch_kwargs_type = BatchKwargs
    recognized_batch_parameters = set()

    def __init__(self, name, datasource) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._name = name
        self._generator_config = {"class_name": self.__class__.__name__}
        self._data_asset_iterators = {}
        if datasource is None:
            raise ValueError("datasource must be provided for a BatchKwargsGenerator")
        self._datasource = datasource

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
        return self._name

    def _get_iterator(self, data_asset_name, **kwargs) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def get_available_data_asset_names(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Return the list of asset names known by this batch kwargs generator.\n\n        Returns:\n            A list of available names\n        "
        raise NotImplementedError

    def get_available_partition_ids(
        self, generator_asset=None, data_asset_name=None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Applies the current _partitioner to the batches available on data_asset_name and returns a list of valid\n        partition_id strings that can be used to identify batches of data.\n\n        Args:\n            data_asset_name: the data asset whose partitions should be returned.\n\n        Returns:\n            A list of partition_id strings\n        "
        raise NotImplementedError

    def get_config(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._generator_config

    def reset_iterator(
        self, generator_asset=None, data_asset_name=None, **kwargs
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (generator_asset and (not data_asset_name)) or (
            (not generator_asset) and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset
        self._data_asset_iterators[data_asset_name] = (
            self._get_iterator(data_asset_name=data_asset_name, **kwargs),
            kwargs,
        )

    def get_iterator(self, generator_asset=None, data_asset_name=None, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (generator_asset and (not data_asset_name)) or (
            (not generator_asset) and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset
        if data_asset_name in self._data_asset_iterators:
            (data_asset_iterator, passed_kwargs) = self._data_asset_iterators[
                data_asset_name
            ]
            if passed_kwargs != kwargs:
                logger.warning(
                    "Asked to yield batch_kwargs using different supplemental kwargs. Please reset iterator to use different supplemental kwargs."
                )
            return data_asset_iterator
        else:
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
            return self._data_asset_iterators[data_asset_name][0]

    def build_batch_kwargs(self, data_asset_name=None, partition_id=None, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if ((not kwargs.get("name")) and (not data_asset_name)) or (
            kwargs.get("name") and data_asset_name
        ):
            raise ValueError("Please provide either name or data_asset_name.")
        if kwargs.get("name"):
            warnings.warn(
                "The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.",
                DeprecationWarning,
            )
            data_asset_name = kwargs.pop("name")
        "The key workhorse. Docs forthcoming."
        if data_asset_name is not None:
            batch_parameters = {"data_asset_name": data_asset_name}
        else:
            batch_parameters = {}
        if partition_id is not None:
            batch_parameters["partition_id"] = partition_id
        batch_parameters.update(kwargs)
        param_keys = set(batch_parameters.keys())
        recognized_params = (
            self.recognized_batch_parameters
            | self._datasource.recognized_batch_parameters
        )
        if not (param_keys <= recognized_params):
            logger.warning(
                "Unrecognized batch_parameter(s): %s"
                % str(param_keys - recognized_params)
            )
        batch_kwargs = self._build_batch_kwargs(batch_parameters)
        batch_kwargs["data_asset_name"] = data_asset_name
        batch_kwargs["datasource"] = self._datasource.name
        return batch_kwargs

    def _build_batch_kwargs(self, batch_parameters) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError

    def yield_batch_kwargs(self, data_asset_name=None, generator_asset=None, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (generator_asset and (not data_asset_name)) or (
            (not generator_asset) and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset
        if data_asset_name not in self._data_asset_iterators:
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
        (data_asset_iterator, passed_kwargs) = self._data_asset_iterators[
            data_asset_name
        ]
        if passed_kwargs != kwargs:
            logger.warning(
                "Asked to yield batch_kwargs using different supplemental kwargs. Resetting iterator to use new supplemental kwargs."
            )
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
            (data_asset_iterator, passed_kwargs) = self._data_asset_iterators[
                data_asset_name
            ]
        try:
            batch_kwargs = next(data_asset_iterator)
            batch_kwargs["datasource"] = self._datasource.name
            return batch_kwargs
        except StopIteration:
            self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
            (data_asset_iterator, passed_kwargs) = self._data_asset_iterators[
                data_asset_name
            ]
            if passed_kwargs != kwargs:
                logger.warning(
                    "Asked to yield batch_kwargs using different batch parameters. Resetting iterator to use different batch parameters."
                )
                self.reset_iterator(data_asset_name=data_asset_name, **kwargs)
                (data_asset_iterator, passed_kwargs) = self._data_asset_iterators[
                    data_asset_name
                ]
            try:
                batch_kwargs = next(data_asset_iterator)
                batch_kwargs["datasource"] = self._datasource.name
                return batch_kwargs
            except StopIteration:
                logger.warning(
                    f"No batch_kwargs found for data_asset_name {data_asset_name}"
                )
                return {}
        except TypeError:
            logger.warning(
                f"Unable to generate batch_kwargs for data_asset_name {data_asset_name}"
            )
            return {}
