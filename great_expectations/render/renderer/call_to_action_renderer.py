

class CallToActionRenderer():
    _document_defaults = {'header': 'What would you like to do next?', 'styling': {'classes': ['border', 'border-info', 'alert', 'alert-info', 'fixed-bottom', 'alert-dismissible', 'fade', 'show', 'm-0', 'rounded-0', 'invisible'], 'attributes': {'id': 'ge-cta-footer', 'role': 'alert'}}}

    @classmethod
    def render(cls, cta_object):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        :param cta_object: dict\n            {\n                "header": # optional, can be a string or string template\n                "buttons": # list of CallToActionButtons\n            }\n        :return: dict\n            {\n                "header": # optional, can be a string or string template\n                "buttons": # list of CallToActionButtons\n            }\n        '
        if (not cta_object.get('header')):
            cta_object['header'] = cls._document_defaults.get('header')
        cta_object['styling'] = cls._document_defaults.get('styling')
        cta_object['tooltip_icon'] = {'template': '$icon', 'params': {'icon': ''}, 'tooltip': {'content': "To disable this footer, set the show_how_to_buttons flag in your project's data_docs_sites config to false."}, 'styling': {'params': {'icon': {'tag': 'i', 'classes': ['m-1', 'fas', 'fa-question-circle']}}}}
        return cta_object
