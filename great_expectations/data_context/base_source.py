class DataSource(object):
    def __init__(self, *args, **kwargs):
        return

    def get_data_asset(self):
        raise NotImplementedError

    def list_data_assets(self):
        raise NotImplementedError
