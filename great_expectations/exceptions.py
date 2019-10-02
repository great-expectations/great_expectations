import os


class GreatExpectationsError(Exception):
    def __init__(self, message):
        self.message = message  


class DataContextError(GreatExpectationsError):
    pass


class InvalidConfigurationYamlError(GreatExpectationsError):
    pass


class InvalidTopLevelConfigKeyError(GreatExpectationsError):
    pass


class MissingTopLevelConfigKeyError(GreatExpectationsError):
    pass


class InvalidConfigValueTypeError(GreatExpectationsError):
    pass


class InvalidConfigVersionError(GreatExpectationsError):
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


class ConfigNotFoundError(DataContextError):
    """The great_expectations dir could not be found."""
    def __init__(self):
        self.message = """Error: No great_expectations directory was found here!
    - Please check that you are in the correct directory or have specified the correct directory.
    - If you have never run Great Expectations in this project, please run `great_expectations init` to get started.
"""


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
