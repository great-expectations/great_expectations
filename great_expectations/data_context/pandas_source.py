import pandas as pd
import os

from .base_source import DataSource
from ..dataset.pandas_dataset import PandasDataset


class PandasCSVDataSource(DataSource):
    """
    A PandasCSVDataContext makes it easy to get a list of files available in the list_datasets
    method. Its get_dataset method returns a new Pandas dataset with the provided name.

    Warning: this feature is new in v0.4 and may change based on community feedback.
    """

    def __init__(self, *args, **kwargs):
        super(PandasCSVDataSource, self).__init__(*args, **kwargs)
        self.connect(kwargs["path"])

    def connect(self, path):
        self.directory = path

    def list_data_assets(self):
        return os.listdir(self.directory)

    def get_data_asset(self, dataset_name, *args, **kwargs):
        data_context = kwargs.pop("data_context", None)
        df = pd.read_csv(os.path.join(
            self.directory, dataset_name), *args, **kwargs)
        return PandasDataset(df, data_context=data_context)
