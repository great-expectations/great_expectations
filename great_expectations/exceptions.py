import os

class GreatExpectationsError(Exception):
    def __init__(self, message):
        self.message = message  

class DataContextError(GreatExpectationsError):
    pass

class ProfilerError(GreatExpectationsError):
    pass

class InvalidConfigError(DataContextError):
    def __init__(self, message):
        self.message = message
    
class ConfigNotFoundError(DataContextError):
    """
    the config file or the environments file cannot be found
    """
    def __init__(self, message):
        self.message = message

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
