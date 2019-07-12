from .batch_generator import BatchGenerator


class InMemoryGenerator(BatchGenerator):
    """A basic generator that simply captures an existing object."""

    def __init__(self, name="default", datasource=None):
        super(InMemoryGenerator, self).__init__(name, type_="memory", datasource=datasource)

    def _get_iterator(self, data_asset_name, **kwargs):
        return iter([])

    def get_available_data_asset_names(self):
        return set()