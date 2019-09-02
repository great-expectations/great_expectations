import copy

from great_expectations.types import (
    AllowedKeysDotDict
)
from great_expectations.data_context.util import (
    instantiate_class_from_config
)

# NOTE: This method will be common to all configurable classes.
# TODO : Assuming we like the pattern, it belongs in great_expectations.types or great_expectations.data_context.types
class Configurable(object):

    # NOTE: I'm torn on whether to pass configs in this fashion or with explicit arguments.
    # I'm also torn about whether to store config-based arguments directly in self or self.config
    
    # This format is the most explicit and concise, so I'm putting it out as the default.
    # It leans into Java-like specification types, rather than pythonic specificication of arguments
    # Is there guidance/best practice for how to apply typing in python 3?
    # We can't be the first to encounter these issues.
    def __init__(self, config, runtime_config):
        # TODO : Trap these errors and raise with informative error messages
        # assert self.config_class exists
        # assert self.runtime_config_class exists
        assert isinstance(config, self.config_class)
        assert isinstance(runtime_config, self.runtime_config_class)

        self.config = config
        self.runtime_config = runtime_config

    # #Here's an alternative way to specify a configurable __init__class
    # This could also be done with clever looping over the config
    # def __init__(self, config, runtime_config): 
    #     self.a = config.a
    #     self.b = config.b
    #     self.c = config.c
    #     self.x = runtime_config.x
    #     self.y = runtime_config.y
    #     self.z = runtime_config.z


    # #Here's an alternative way to specify a configurable __init__class
    # #This could also be done with clever looping over args and kwargs
    # def __init__(self, a, x, b=None, c=None, y=None, z=None):
    #     self.a = a
    #     self.b = b
    #     self.c = c
    #     self.x = x
    #     self.y = y
    #     self.z = z

# These config classes use AllowedKeysDotDict, but could just as easily be based on classes that use properties, etc.
class FakeConfigurableClassConfig(AllowedKeysDotDict):
    _allowed_keys = set(["a", "b", "c"])
    _required_keys = set(["a"])

# configs and runtime_configs need to be specified separately, because different information may be available at different times.
# Currently, the only common element in runtim_config is "root_directory" aka "project_directory": the base directory for the GE project.
# But it's likely that there will be others in the future, as we start deploying GE in more environments.
class FakeConfigurableClassRuntimeConfig(AllowedKeysDotDict):
    _allowed_keys = set(["x", "y", "z"])
    _required_keys = set(["x"])

class FakeConfigurableClass(Configurable):
    # NOTE: This configuration pattern requires all configurable classes to keep a handle to their config type.
    config_class = FakeConfigurableClassConfig
    runtime_config_class = FakeConfigurableClassRuntimeConfig

    # This is a shell class, only used for testing configs.
    # If it were a real class, it would have, like, methods and stuff.

class FakeConfigurableWrapperClassConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "foo",
        "fake_configurable_class_config",
    ])
    _required_keys = set([
        "foo",
        "fake_configurable_class_config"
    ])
    _key_types = {
        "foo" : int,
        "fake_configurable_class_config" : dict, # NOTE: This is a dict, NOT a FakeConfigurableClassConfig. This allows us to inject default values
    }

class FakeConfigurableWrapperClass(Configurable):
    config_class = FakeConfigurableWrapperClassConfig
    runtime_config_class = FakeConfigurableClassRuntimeConfig

    def __init__(self, config, runtime_config):
        super(FakeConfigurableWrapperClass, self).__init__(config, runtime_config)

        # This code allows us to specify defaults
        fake_configurable_class_config = copy.deepcopy(config.fake_configurable_class_config)
        self.fake_configurable_object = instantiate_class_from_config(
            fake_configurable_class_config,
            self.runtime_config,
            config_defaults={
                "a" : "default_value_for_a"
            }
        )