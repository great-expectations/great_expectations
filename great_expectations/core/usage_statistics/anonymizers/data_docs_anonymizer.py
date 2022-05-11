
from typing import Optional
from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

class DataDocsAnonymizer(BaseAnonymizer):

    def __init__(self, aggregate_anonymizer: 'Anonymizer', salt: Optional[str]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        super().__init__(salt=salt)
        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(self, site_name: str, site_config: dict, obj: Optional[object]=None) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        site_config_module_name = site_config.get('module_name')
        if (site_config_module_name is None):
            site_config['module_name'] = 'great_expectations.render.renderer.site_builder'
        anonymized_info_dict = self._anonymize_site_builder_info(site_builder_config=site_config)
        anonymized_info_dict['anonymized_name'] = self._anonymize_string(site_name)
        store_backend_config = site_config.get('store_backend')
        anonymized_info_dict['anonymized_store_backend'] = self._aggregate_anonymizer.anonymize(store_backend_object_config=store_backend_config)
        site_index_builder_config = site_config.get('site_index_builder')
        anonymized_site_index_builder = self._anonymize_site_builder_info(site_builder_config=site_index_builder_config)
        if ('show_cta_footer' in site_index_builder_config):
            anonymized_site_index_builder['show_cta_footer'] = site_index_builder_config.get('show_cta_footer')
        anonymized_info_dict['anonymized_site_index_builder'] = anonymized_site_index_builder
        return anonymized_info_dict

    def _anonymize_site_builder_info(self, site_builder_config: dict) -> dict:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        class_name = site_builder_config.get('class_name')
        module_name = site_builder_config.get('module_name')
        if (module_name is None):
            module_name = 'great_expectations.render.renderer.site_builder'
        anonymized_info_dict = {}
        self._anonymize_object_info(object_config={'class_name': class_name, 'module_name': module_name}, anonymized_info_dict=anonymized_info_dict)
        return anonymized_info_dict

    def can_handle(self, obj: Optional[object]=None, **kwargs) -> bool:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return ('site_name' and ('site_config' in kwargs))
