
import logging
from ruamel.yaml import YAML
from great_expectations.data_context.data_context.data_context import DataContext
logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

class ExplorerDataContext(DataContext):

    def __init__(self, context_root_dir=None, expectation_explorer=True) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n            expectation_explorer: If True, load the expectation explorer manager, which will modify GE return objects             to include ipython notebook widgets.\n        '
        super().__init__(context_root_dir)
        self._expectation_explorer = expectation_explorer
        if expectation_explorer:
            from great_expectations.jupyter_ux.expectation_explorer import ExpectationExplorer
            self._expectation_explorer_manager = ExpectationExplorer()

    def update_return_obj(self, data_asset, return_obj):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Helper called by data_asset.\n\n        Args:\n            data_asset: The data_asset whose validation produced the current return object\n            return_obj: the return object to update\n\n        Returns:\n            return_obj: the return object, potentially changed into a widget by the configured expectation explorer\n        '
        if self._expectation_explorer:
            return self._expectation_explorer_manager.create_expectation_widget(data_asset, return_obj)
        else:
            return return_obj
