from marshmallow import ValidationError


class GreatExpectationsError(Exception):
    def __init__(self, message):
        self.message = message


class GreatExpectationsValidationError(ValidationError, GreatExpectationsError):
    def __init__(self, message, validation_error):
        self.message = message
        self.messages = validation_error.messages
        self.data = validation_error.data
        self.field_names = validation_error.field_names
        self.fields = validation_error.fields
        self.kwargs = validation_error.kwargs


class DataContextError(GreatExpectationsError):
    pass


class InvalidConfigurationYamlError(GreatExpectationsError):
    pass


class InvalidTopLevelConfigKeyError(GreatExpectationsError):
    pass


class MissingTopLevelConfigKeyError(GreatExpectationsValidationError):
    pass


class InvalidDataContextConfigError(GreatExpectationsValidationError):
    pass


class InvalidBatchKwargsError(GreatExpectationsError):
    pass


class InvalidBatchIdError(GreatExpectationsError):
    pass


class InvalidDataContextKeyError(DataContextError):
    pass


class UnsupportedConfigVersionError(GreatExpectationsError):
    pass


class ZeroDotSevenConfigVersionError(GreatExpectationsError):
    pass


class ProfilerError(GreatExpectationsError):
    pass


class InvalidConfigError(DataContextError):
    def __init__(self, message):
        self.message = message


class StoreConfigurationError(DataContextError):
    pass


class InvalidExpectationKwargsError(GreatExpectationsError):
    pass


class InvalidExpectationConfigurationError(GreatExpectationsError):
    pass


class InvalidValidationResultError(GreatExpectationsError):
    pass


class GreatExpectationsTypeError(TypeError):
    pass


class ConfigNotFoundError(DataContextError):
    """The great_expectations dir could not be found."""
    def __init__(self):
        self.message = """Error: No great_expectations directory was found here!
    - Please check that you are in the correct directory or have specified the correct directory.
    - If you have never run Great Expectations in this project, please run `great_expectations init` to get started.
"""


class PluginModuleNotFoundError(GreatExpectationsError):
    """A module import failed."""
    def __init__(self, module_name):
        template = """\
Error: No module named `{}` could be found in your plugins directory.
    - Please verify your plugins directory is configured correctly.
    - Please verify you have a module named `{}` in your plugins directory.
"""
        self.message = template.format(module_name, module_name)

        colored_template = "<red>" + template + "</red>"
        module_snippet = "</red><yellow>" + module_name + "</yellow><red>"
        self.cli_colored_message = colored_template.format(
            module_snippet,
            module_snippet
        )


class PluginClassNotFoundError(GreatExpectationsError, AttributeError):
    """A module import failed."""
    def __init__(self, module_name, class_name):
        template = """Error: The module: `{}` does not contain the class: `{}`.
    - Please verify this class name `{}`.
"""
        self.message = template.format(module_name, class_name, class_name)

        colored_template = "<red>" + template + "</red>"
        module_snippet = "</red><yellow>" + module_name + "</yellow><red>"
        class_snippet = "</red><yellow>" + class_name + "</yellow><red>"
        self.cli_colored_message = colored_template.format(
            module_snippet,
            class_snippet,
            class_snippet,
        )


class ExpectationSuiteNotFoundError(GreatExpectationsError):
    def __init__(self, data_asset_name):
        self.data_asset_name = data_asset_name
        self.message = "No expectation suite found for data_asset_name %s" % data_asset_name


class BatchKwargsError(DataContextError):
    def __init__(self, message, batch_kwargs):
        self.message = message
        self.batch_kwargs = batch_kwargs


class DatasourceInitializationError(GreatExpectationsError):
    def __init__(self, datasource_name, message):
        self.message = "Cannot initialize datasource %s, error: %s" % (datasource_name, message)


class InvalidConfigValueTypeError(DataContextError):
    pass
