from .base import DataContext
from .. import read_csv

import glob

class PandasCSVDataContext(DataContext):
    """
    A PandasCSVDataContext makes it easy to get a list of files available in the list_datasets
    method. Its get_dataset method returns a new Pandas dataset with the provided name.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, *args, **kwargs):
        super(PandasCSVDataContext, self).__init__(*args, **kwargs)

    def connect(self, options):
        self.directory = options

    def list_datasets(self):
        return glob.glob(self.directory)

    def get_dataset(self, dataset_name):
        df = read_csv(dataset_name)
        return df