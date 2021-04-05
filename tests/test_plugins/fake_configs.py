from great_expectations.data_context.util import instantiate_class_from_config


class FakeConfigurableClass:
    def __init__(self, a, x, b=None, c=None, y=None, z=None):
        self.a = a
        self.b = b
        self.c = c

        self.x = x
        self.y = y
        self.z = z


class FakeConfigurableWrapperClass:
    def __init__(
        self,
        foo,
        fake_configurable,
        x,
        y=None,
        z=None,
    ):
        assert isinstance(foo, int)

        self.foo = foo

        self.x = x
        self.y = y
        self.z = z

        print(fake_configurable)

        # This code allows us to specify defaults for the child class
        self.fake_configurable_object = instantiate_class_from_config(
            config=fake_configurable,
            runtime_environment={
                "x": self.x,
                "y": self.y,
                "z": self.z,
            },
            config_defaults={"a": "default_value_for_a"},
        )
