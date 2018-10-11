import pandas as pd
import os

from .base import DataContext
from ..dataset.pandas_dataset import PandasDataset


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
        return os.listdir(self.directory)

    def get_dataset(self, dataset_name, *args, **kwargs):
        df = pd.read_csv(os.path.join(
            self.directory, dataset_name), *args, **kwargs)
        return PandasDataset(df)
