import os
import logging
import traceback
import copy
from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.compat import StringIO

from great_expectations.data_context.util import (
    substitute_all_config_variables,
    instantiate_class_from_config,
)

from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.types.base import dataContextConfigSchema

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

class DataContextV3(DataContext):
    """Class implementing the v3 spec for DataContext configs, plus API changes for the 0.13+ series."""

    def get_config(self, mode="typed"):
        config = super().get_config()

        if mode=="typed":
            return config

        elif mode=="commented_map":
            return config.commented_map

        elif mode=="dict":
            return dict(config.commented_map)

        elif mode=="yaml":
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
        return dict(
            self._load_config_variables_file()
        )

    @property
    def datasources(self):
        """A single holder for all Datasources in this context"""
        return self._cached_datasources

    def test_yaml_config(
        self,
        yaml_config: str,
        name = None,
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
            self.config_variables,
            dict(os.environ),
        )

        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }

        config_str_with_substituted_variables = substitute_all_config_variables(
            yaml_config,
            substitutions,
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
                instantiated_class = self._build_store_from_config("my_temp_store", config)

            elif class_name in ["ExecutionEnvironment", "StreamlinedSqlExecutionEnvironment"]:
                print(
                    f"\tInstantiating as a ExecutionEnvironment, since class_name is {class_name}"
                )
                execution_environment_name = name or "my_temp_execution_environment"
                instantiated_class = self._build_execution_environment_from_config(
                    execution_environment_name,
                    config,
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
                raise(e)

    def get_validator(
        self,
        execution_environment_name: str=None,
        data_connector_name: str=None,
        data_asset_name: str=None,
        batch_definition: BatchDefinition=None,
        batch_request: BatchRequest=None,
        partition_request: Union[PartitionRequest, dict]=None,
        partition_identifiers: dict=None,
        limit: int=None,
        index=None,
        custom_filter_function: Callable=None,
        sampling_method: str=None,
        sampling_kwargs: dict=None,
        expectation_suite_name: str=None,
        expectation_suite: ExpectationSuite=None,
        **kwargs,
    ):
        if expectation_suite_name is None:
            if not expectation_suite_name is None:
                expectation_suite = self.get_expectation_suite(expectation_suite_name)
            else:
                raise ValueError("expectation_suite and expectation_suite_name cannot both be None")

        else:
            if not expectation_suite_name is None:
                raise Warning("get_validator recieved values for both expectation_suite and expectation_suite_name. Defaulting to expectation_suite.")

        batch = self.get_batch_from_new_style_datasource(
            execution_environment_name,
            data_connector_name,
            data_asset_name,
            batch_definition,
            batch_request,
            partition_request,
            partition_identifiers,
            limit,
            index,
            custom_filter_function,
            sampling_method,
            sampling_kwargs,
            **kwargs,
        )