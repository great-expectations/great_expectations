import os

class GreatExpectationsError(Exception):
    def __init__(self, message):
        self.message = message  

class DataContextError(GreatExpectationsError):
    pass

class ProfilerError(GreatExpectationsError):
    pass
    
class ConfigNotFoundError(DataContextError):
    def __init__(self, context_root_directory):
        self.message = "No configuration found in %s" % str(os.path.join(context_root_directory, "great_expectations"))

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

