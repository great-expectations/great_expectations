from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

from great_expectations import DataContext
from great_expectations.rule_based_profiler.config.base import (
    ruleBasedProfilerConfigSchema,
)
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

yaml = YAML()


def test_yaml_simple_date_format_string_parameter_builder(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    # Load data context
    data_context: DataContext = alice_columnar_table_single_batch_context

    # Load profiler configs & loop (run tests for each one)
    yaml_config: str = alice_columnar_table_single_batch["profiler_config"]

    # Instantiate Profiler
    profiler_config: CommentedMap = yaml.load(yaml_config)

    # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
    deserialized_config: dict = ruleBasedProfilerConfigSchema.load(profiler_config)
    serialized_config: dict = ruleBasedProfilerConfigSchema.dump(deserialized_config)

    # `class_name`/`module_name` are generally consumed through `instantiate_class_from_config`
    # so we need to manually remove those values if we wish to use the **kwargs instantiation pattern
    serialized_config.pop("class_name")
    serialized_config.pop("module_name")

    profiler: RuleBasedProfiler = RuleBasedProfiler(
        **serialized_config,
        data_context=data_context,
    )

    assert profiler.rules[1].parameter_builders[3]["name"] == "my_date_format"
