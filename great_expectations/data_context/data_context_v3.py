from .data_context import DataContext

class DataContextV3(DataContext):
    """Class implementing the v3 spec for DataContext configs, plus API changes for the 0.13+ series."""
    
    @property
    def config_variables(self):
        # Note Abe 20121114 : We should probably cache config_variables instead of loading them from disk every time.
        return dict(
            self._load_config_variables_file()
        )