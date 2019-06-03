class GreatExpectationsError(Exception):
    pass
  
  
class ExpectationsConfigNotFoundError(GreatExpectationsError):
    def __init__(self, data_asset_name):
        self.data_asset_name = data_asset_name
        self.message = "No expectations config found for data_asset_name %s" % data_asset_name


class BatchKwargsError(GreatExpectationsError):
    def __init__(self, message, batch_kwargs):
        self.message = message