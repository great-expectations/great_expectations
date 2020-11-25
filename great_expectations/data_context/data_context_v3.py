import copy
import logging
import os
import traceback
from typing import Callable, Optional, Union

from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.compat import StringIO

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchRequest,
    PartitionRequest,
)
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.types.base import dataContextConfigSchema
from great_expectations.data_context.util import (
    instantiate_class_from_config,
    substitute_all_config_variables,
)
from great_expectations.datasource.new_datasource import BaseDatasource, Datasource
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class DataContextV3(DataContext):
    """Class implementing the v3 spec for DataContext configs, plus API changes for the 0.13+ series."""

    def get_config(self, mode="typed"):
        config = super().get_config()

        if mode == "typed":
            return config

        elif mode == "commented_map":
            return config.commented_map

        elif mode == "dict":
            return dict(config.commented_map)

        elif mode == "yaml":
            commented_map = copy.deepcopy(config.commented_map)
            commented_map.update(dataContextConfigSchema.dump(config))

            stream = StringIO()
            yaml.dump(commented_map, stream)
            yaml_string = stream.getvalue()

            # print(commented_map)
            # print(commented_map.__dict__)
            # print(str(commented_map))
            return yaml_string
            # config.commented_map.update(dataContextConfigSchema.dump(self))

        else:
            raise ValueError(f"Unknown config mode {mode}")

    @property
    def config_variables(self):
        # Note Abe 20121114 : We should probably cache config_variables instead of loading them from disk every time.
        return dict(self._load_config_variables_file())

    # TODO: <Alex>We need to standardize the signatures of methods in all subclasses of BaseDataContext</Alex>
    # TODO: <Alex>Placing this method here avoids conflict with those in DataContext, handling LegacyDatasource</Alex>
    def add_datasource(self, datasource_name, datasource_config):
        logger.debug(
            "Starting DataContext.add_datasource for datasource %s" % datasource_name
        )

        new_datasource = self._build_and_add_datasource(
            datasource_name, datasource_config
        )
        self._save_project_config()

        return new_datasource

    # TODO: <Alex>Placing this method here avoids conflict with those in DataContext, handling LegacyDatasource</Alex>
    def _build_and_add_datasource(self, datasource_name, datasource_config):
        """Add a new Store to the DataContext and (for convenience) return the instantiated Store object.

        Args:
            datasource_name (str): a key for the new Datasource in in self._datasources
            datasource_config (dict): a config for the Datasource to add

        Returns:
            datasource (Datasource)
        """

        new_datasource = self._build_datasource_from_config(
            datasource_name, datasource_config,
        )
        self._project_config["datasources"][datasource_name] = datasource_config
        return new_datasource

    # TODO: <Alex>Placing this method here avoids conflict with those in DataContext, handling LegacyDatasource</Alex>
    def _build_datasource_from_config(self, name: str, config: dict,) -> BaseDatasource:
        module_name: str = "great_expectations.datasource"
        runtime_environment: dict = {
            "name": name,
            "data_context_root_directory": self.root_directory,
        }
        new_datasource: BaseDatasource = instantiate_class_from_config(
            config=config,
            runtime_environment=runtime_environment,
            config_defaults={"module_name": module_name},
        )

        if not new_datasource:
            raise ge_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=config["class_name"],
            )

        if not isinstance(new_datasource, BaseDatasource):
            raise TypeError(
                f"Newly instantiated component {name} is not an instance of BaseDatasource. Please check class_name in the config."
            )

        self._cached_datasources[name] = new_datasource
        return new_datasource

    @property
    def datasources(self):
        """A single holder for all Datasources in this context"""
        return self._cached_datasources

    def test_yaml_config(
        self,
        yaml_config: str,
        name=None,
        pretty_print=True,
        return_mode="instantiated_class",
        shorten_tracebacks=False,
    ):
        """ Convenience method for testing yaml configs

        test_yaml_config is a convenience method for configuring the moving
        parts of a Great Expectations deployment. It allows you to quickly
        test out configs for system components, especially Datasources,
        Checkpoints, and Stores.

        For many deployments of Great Expectations, these components (plus
        Expectations) are the only ones you'll need.

        test_yaml_config is mainly intended for use within notebooks and tests.

        Parameters
        ----------
        yaml_config : str
            A string containing the yaml config to be tested

        name: str
            (Optional) A string containing the name of the component to instantiate

        pretty_print : bool
            Determines whether to print human-readable output

        return_mode : str
            Determines what type of object test_yaml_config will return
            Valid modes are "instantiated_class" and "report_object"

        shorten_tracebacks : bool
            If true, catch any errors during instantiation and print only the
            last element of the traceback stack. This can be helpful for
            rapid iteration on configs in a notebook, because it can remove
            the need to scroll up and down a lot.

        Returns
        -------
        The instantiated component (e.g. a Datasource)
        OR
        a json object containing metadata from the component's self_check method

        The returned object is determined by return_mode.
        """
        if pretty_print:
            print("Attempting to instantiate class from config...")

        if not return_mode in ["instantiated_class", "report_object"]:
            raise ValueError(f"Unknown return_mode: {return_mode}.")

        substituted_config_variables = substitute_all_config_variables(
            self.config_variables, dict(os.environ),
        )

        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }

        config_str_with_substituted_variables = substitute_all_config_variables(
            yaml_config, substitutions,
        )

        config = yaml.load(config_str_with_substituted_variables)

        if "class_name" in config:
            class_name = config["class_name"]
        else:
            class_name = None

        try:
            if class_name in [
                "ExpectationsStore",
                "ValidationsStore",
                "HtmlSiteStore",
                "EvaluationParameterStore",
                "MetricStore",
                "SqlAlchemyQueryStore",
            ]:
                print(f"\tInstantiating as a Store, since class_name is {class_name}")
                instantiated_class = self._build_store_from_config(
                    "my_temp_store", config
                )

            elif class_name in [
                "Datasource",
                "SimpleSqlalchemyDatasource",
            ]:
                print(
                    f"\tInstantiating as a Datasource, since class_name is {class_name}"
                )
                datasource_name = name or "my_temp_datasource"
                instantiated_class = self._build_datasource_from_config(
                    datasource_name, config,
                )

            else:
                print(
                    "\tNo matching class found. Attempting to instantiate class from the raw config..."
                )
                instantiated_class = instantiate_class_from_config(
                    config, runtime_environment={}, config_defaults={}
                )

            if pretty_print:
                print(
                    f"\tSuccessfully instantiated {instantiated_class.__class__.__name__}"
                )
                print()

            report_object = instantiated_class.self_check(pretty_print)

            if return_mode == "instantiated_class":
                return instantiated_class

            elif return_mode == "report_object":
                return report_object

        except Exception as e:
            if shorten_tracebacks:
                traceback.print_exc(limit=1)

            else:
                raise (e)

    def get_batch(
        self,
        datasource_name: str = None,
        data_connector_name: str = None,
        data_asset_name: str = None,
        *,
        batch_request: BatchRequest = None,
        partition_request: Union[PartitionRequest, dict] = None,
        partition_identifiers: dict = None,
        limit: int = None,
        index=None,
        custom_filter_function: Callable = None,
        batch_spec_passthrough: Optional[dict] = None,
        sampling_method: str = None,
        sampling_kwargs: dict = None,
        splitter_method: str = None,
        splitter_kwargs: dict = None,
        **kwargs,
    ) -> Batch:
        """Get exactly one batch, based on a variety of flexible input types.

        Args:
            batch_request

            datasource_name
            data_connector_name
            data_asset_name

            batch_request
            partition_request
            partition_identifiers

            limit
            index
            custom_filter_function

            sampling_method
            sampling_kwargs

            splitter_method
            splitter_kwargs

            batch_spec_passthrough

            **kwargs

        Returns:
            (Batch) The requested batch

        `get_batch` is the main user-facing API for getting batches.
        In contrast to virtually all other methods in the class, it does not require typed or nested inputs.
        Instead, this method is intended to help the user pick the right parameters

        This method attempts returns exactly one batch.
        If 0 or more than batches would be returned, it raises an error.
        """

        if batch_request:
            if not isinstance(batch_request, BatchRequest):
                raise TypeError(
                    f"batch_request must be an instance of BatchRequest object, not {type(batch_request)}"
                )
            datasource_name = batch_request.datasource_name
        else:
            datasource_name = datasource_name

        datasource = self.datasources[datasource_name]

        if batch_request:
            # TODO: Raise a warning if any parameters besides batch_requests are specified
            return datasource.get_single_batch_from_batch_request(
                batch_request=batch_request
            )
        else:
            partition_request: PartitionRequest
            if partition_request is None:
                if partition_identifiers is None:
                    partition_identifiers = kwargs
                else:
                    # Raise a warning if kwargs exist
                    pass

                # Currently, the implementation of splitting and sampling is inconsistent between the
                # Datasource and SimpleSqlalchemyDatasource classes.  The former communicates these
                # directives to the underlying ExecutionEngine objects via "batch_spec_passthrough", which ultimately
                # gets merged with "batch_spec" and processed by the configured ExecutionEngine object.  However,
                # SimpleSqlalchemyDatasource uses "PartitionRequest" to relay the splitting and sampling
                # directives to the SqlAlchemyExecutionEngine object.  The problem with this is that if the querying
                # of partitions is implemented using the PartitionQuery class, it will not recognized the keys
                # representing the splitting and sampling directives and raise an exception.  Additional work is needed
                # to decouple the directives that go into PartitionQuery from the other PartitionRequest directives.
                partition_request_params: dict = {
                    "partition_identifiers": partition_identifiers,
                    "limit": limit,
                    "index": index,
                    "custom_filter_function": custom_filter_function,
                }
                if sampling_method is not None:
                    sampling_params: dict = {
                        "sampling_method": sampling_method,
                    }
                    if sampling_kwargs is not None:
                        sampling_params["sampling_kwargs"] = sampling_kwargs
                    partition_request_params.update(sampling_params)
                if splitter_method is not None:
                    splitter_params: dict = {
                        "splitter_method": splitter_method,
                    }
                    if splitter_kwargs is not None:
                        splitter_params["splitter_kwargs"] = splitter_kwargs
                    partition_request_params.update(splitter_params)
                partition_request = PartitionRequest(partition_request_params)
            else:
                # Raise a warning if partition_identifiers or kwargs exist
                partition_request = PartitionRequest(partition_request)

            batch_request: BatchRequest = BatchRequest(
                datasource_name=datasource_name,
                data_connector_name=data_connector_name,
                data_asset_name=data_asset_name,
                partition_request=partition_request,
                batch_spec_passthrough=batch_spec_passthrough,
            )
            return datasource.get_single_batch_from_batch_request(
                batch_request=batch_request
            )

    def get_validator(
        self,
        datasource_name: str = None,
        data_connector_name: str = None,
        data_asset_name: str = None,
        *,
        batch_request: BatchRequest = None,
        partition_request: Union[PartitionRequest, dict] = None,
        partition_identifiers: dict = None,
        limit: int = None,
        index=None,
        custom_filter_function: Callable = None,
        expectation_suite_name: str = None,
        expectation_suite: ExpectationSuite = None,
        create_expectation_suite_with_name: str = None,
        batch_spec_passthrough: Optional[dict] = None,
        sampling_method: str = None,
        sampling_kwargs: dict = None,
        splitter_method: str = None,
        splitter_kwargs: dict = None,
        **kwargs,
    ) -> Validator:
        if (
            sum(
                bool(x)
                for x in [
                    expectation_suite is not None,
                    expectation_suite_name is not None,
                    create_expectation_suite_with_name is not None,
                ]
            )
            != 1
        ):
            raise ValueError(
                "Exactly one of expectation_suite_name, expectation_suite, or create_expectation_suite_with_name must be specified"
            )

        if expectation_suite_name is not None:
            expectation_suite = self.get_expectation_suite(expectation_suite_name)

        if create_expectation_suite_with_name is not None:
            expectation_suite = self.create_expectation_suite(
                expectation_suite_name=create_expectation_suite_with_name
            )

        batch = self.get_batch(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_request=batch_request,
            partition_request=partition_request,
            partition_identifiers=partition_identifiers,
            limit=limit,
            index=index,
            custom_filter_function=custom_filter_function,
            batch_spec_passthrough=batch_spec_passthrough,
            sampling_method=sampling_method,
            sampling_kwargs=sampling_kwargs,
            splitter_method=splitter_method,
            splitter_kwargs=splitter_kwargs,
            **kwargs,
        )

        batch_definition = batch.batch_definition
        execution_engine = self.datasources[
            batch_definition.datasource_name
        ].execution_engine

        validator = Validator(
            execution_engine=execution_engine,
            interactive_evaluation=True,
            expectation_suite=expectation_suite,
            data_context=self,
            batches=[batch],
        )

        return validator
