import pytest

from great_expectations.data_context.util import instantiate_class_from_config


def test_instantiate_class_from_config():
    # This config structure feels very tidy to me.
    instantiate_class_from_config(
        config={
            "module_name": "tests.test_plugins.fake_configs",
            "class_name": "FakeConfigurableClass",
            "a": "hi",
        },
        runtime_environment={"x": 1},
    )


def test_instantiate_class_from_config_with_overriden_defaults():

    # This config structure feels very tidy to me, even with nesting.
    fake_configurable_wrapper_object = instantiate_class_from_config(
        config={
            "module_name": "tests.test_plugins.fake_configs",
            "class_name": "FakeConfigurableWrapperClass",
            "foo": 100,
            "fake_configurable": {
                "module_name": "tests.test_plugins.fake_configs",
                "class_name": "FakeConfigurableClass",
            },
        },
        runtime_environment={"x": 1},
    )
    assert (
        fake_configurable_wrapper_object.fake_configurable_object.a
        == "default_value_for_a"
    )

    fake_configurable_wrapper_object = instantiate_class_from_config(
        config={
            "module_name": "tests.test_plugins.fake_configs",
            "class_name": "FakeConfigurableWrapperClass",
            "foo": 100,
            "fake_configurable": {
                "module_name": "tests.test_plugins.fake_configs",
                "class_name": "FakeConfigurableClass",
                "a": "not_the_default_value",
            },
        },
        runtime_environment={"x": 1},
    )
    assert (
        fake_configurable_wrapper_object.fake_configurable_object.a
        == "not_the_default_value"
    )


def test_instantiate_class_from_config_with_config_defaults():
    my_runtime_environment = {"x": 1}

    # `a` specified in config only. No default
    fake_configurable_object = instantiate_class_from_config(
        config={
            "class_name": "FakeConfigurableClass",
            "module_name": "tests.test_plugins.fake_configs",
            "a": "value_from_the_config",
        },
        runtime_environment=my_runtime_environment,
        config_defaults={},
    )
    assert fake_configurable_object.a == "value_from_the_config"

    # Default only. No config
    fake_configurable_object = instantiate_class_from_config(
        config={
            "class_name": "FakeConfigurableClass",
            "module_name": "tests.test_plugins.fake_configs",
        },
        runtime_environment=my_runtime_environment,
        config_defaults={"a": "value_from_the_defaults",},
    )
    assert fake_configurable_object.a == "value_from_the_defaults"

    # Both.
    fake_configurable_object = instantiate_class_from_config(
        config={
            "class_name": "FakeConfigurableClass",
            "module_name": "tests.test_plugins.fake_configs",
            "a": "value_from_the_config",
        },
        runtime_environment=my_runtime_environment,
        config_defaults={"a": "value_from_the_defaults",},
    )
    assert fake_configurable_object.a == "value_from_the_config"

    # Neither
    with pytest.raises(TypeError):
        fake_configurable_object = instantiate_class_from_config(
            config={
                "class_name": "FakeConfigurableClass",
                "module_name": "tests.test_plugins.fake_configs",
            },
            runtime_environment=my_runtime_environment,
            config_defaults={},
        )

    # Module name specified in default, but not config.
    fake_configurable_object = instantiate_class_from_config(
        config={"class_name": "FakeConfigurableClass", "a": "value_from_the_config",},
        runtime_environment=my_runtime_environment,
        config_defaults={"module_name": "tests.test_plugins.fake_configs",},
    )

    # Both
    fake_configurable_object = instantiate_class_from_config(
        config={
            "module_name": "tests.test_plugins.fake_configs",
            "class_name": "FakeConfigurableClass",
            "a": "value_from_the_config",
        },
        runtime_environment=my_runtime_environment,
        config_defaults={"module_name": "tests.test_plugins.fake_configs",},
    )

    # Neither
    with pytest.raises(KeyError):
        fake_configurable_object = instantiate_class_from_config(
            config={
                "class_name": "FakeConfigurableClass",
                "a": "value_from_the_config",
            },
            runtime_environment=my_runtime_environment,
            config_defaults={},
        )

    # Pushing the limits of what we can do with this API...
    fake_configurable_object = instantiate_class_from_config(
        config={"class_name": "FakeConfigurableClass",},
        runtime_environment=my_runtime_environment,
        config_defaults={
            "module_name": "tests.test_plugins.fake_configs",
            "a": "value_from_the_config",
        },
    )

    # This seems like too much magic, but maybe we'll find a place where it's needed.
    fake_configurable_object = instantiate_class_from_config(
        config={"a": "value_from_the_config",},
        runtime_environment=my_runtime_environment,
        config_defaults={
            "module_name": "tests.test_plugins.fake_configs",
            "class_name": "FakeConfigurableClass",
        },
    )

    # Okay, this is just getting silly
    fake_configurable_object = instantiate_class_from_config(
        config={},
        runtime_environment=my_runtime_environment,
        config_defaults={
            "module_name": "tests.test_plugins.fake_configs",
            "class_name": "FakeConfigurableClass",
            "a": "value_from_the_config",
        },
    )
