class DataContext(object):
    """A generic DataContext, exposing the base API including constructor with `options` parameter, list_datasets,
    and get_dataset.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, options, *args, **kwargs):
        self.connect(options, *args, **kwargs)

    def connect(self, options):
        return NotImplementedError

    def list_datasets(self):
        return NotImplementedError

    def get_dataset(self, dataset_name):
        return NotImplementedError
