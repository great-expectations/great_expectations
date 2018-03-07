class DataContext(object):
    """A generic DataContext, exposing the base API including constructor with `options` parameter, list_datasets,
    and get_dataset.

    """
    def __init__(self, options):
        self.connect(self, options)

    def connect(self, options):
        return NotImplementedError

    def list_datasets(self):
        return NotImplementedError

    def get_data_set(self, dataset_name):
        return NotImplementedError
