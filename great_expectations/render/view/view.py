
import datetime
import json
import re
from collections import OrderedDict
from string import Template as pTemplate
from uuid import uuid4
import mistune
from jinja2 import ChoiceLoader, Environment, FileSystemLoader, PackageLoader, contextfilter, select_autoescape
from great_expectations import __version__ as ge_version
from great_expectations.render.types import RenderedComponentContent, RenderedContent, RenderedDocumentContent

class NoOpTemplate():

    def render(self, document):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return document

class PrettyPrintTemplate():

    def render(self, document, indent=2) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        print(json.dumps(document, indent=indent))

class DefaultJinjaView():
    '\n    Defines a method for converting a document to human-consumable form\n\n    Dependencies\n    ~~~~~~~~~~~~\n    * Font Awesome 5.10.1\n    * Bootstrap 4.3.1\n    * jQuery 3.2.1\n    * Vega 5\n    * Vega-Lite 4\n    * Vega-Embed 6\n\n    '
    _template = NoOpTemplate

    def __init__(self, custom_styles_directory=None, custom_views_directory=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.custom_styles_directory = custom_styles_directory
        self.custom_views_directory = custom_views_directory
        templates_loader = PackageLoader('great_expectations', 'render/view/templates')
        styles_loader = PackageLoader('great_expectations', 'render/view/static/styles')
        loaders = [templates_loader, styles_loader]
        if self.custom_styles_directory:
            loaders.append(FileSystemLoader(self.custom_styles_directory))
        if self.custom_views_directory:
            loaders.append(FileSystemLoader(self.custom_views_directory))
        self.env = Environment(loader=ChoiceLoader(loaders), autoescape=select_autoescape(['html', 'xml']), extensions=['jinja2.ext.do'])
        self.env.filters['render_string_template'] = self.render_string_template
        self.env.filters['render_styling_from_string_template'] = self.render_styling_from_string_template
        self.env.filters['render_styling'] = self.render_styling
        self.env.filters['render_content_block'] = self.render_content_block
        self.env.filters['render_markdown'] = self.render_markdown
        self.env.filters['get_html_escaped_json_string_from_dict'] = self.get_html_escaped_json_string_from_dict
        self.env.filters['generate_html_element_uuid'] = self.generate_html_element_uuid
        self.env.filters['attributes_dict_to_html_string'] = self.attributes_dict_to_html_string
        self.env.filters['render_bootstrap_table_data'] = self.render_bootstrap_table_data
        self.env.globals['ge_version'] = ge_version
        self.env.filters['add_data_context_id_to_url'] = self.add_data_context_id_to_url

    def render(self, document, template=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self._validate_document(document)
        if (template is None):
            template = self._template
        t = self._get_template(template)
        if isinstance(document, RenderedContent):
            document = document.to_json_dict()
        return t.render(document, **kwargs)

    def _get_template(self, template):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (template is None):
            return NoOpTemplate
        template = self.env.get_template(template)
        template.globals['now'] = (lambda : datetime.datetime.now(datetime.timezone.utc))
        return template

    @contextfilter
    def add_data_context_id_to_url(self, jinja_context, url, add_datetime=True):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        data_context_id = jinja_context.get('data_context_id')
        if add_datetime:
            datetime_iso_string = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
            url += f'?d={datetime_iso_string}'
        if data_context_id:
            url = (f'{url}&dataContextId=' if add_datetime else f'{url}?dataContextId=')
            url += data_context_id
        return url

    @contextfilter
    def render_content_block(self, jinja_context, content_block, index=None, content_block_id=None, render_to_markdown: bool=False):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n\n        :param jinja_context:\n        :param content_block:\n        :param index:\n        :param content_block_id:\n        :param render_to_markdown: Whether this method should render the markdown version instead of HTML\n        :return:\n        '
        if isinstance(content_block, str):
            return content_block
        elif (content_block is None):
            return ''
        elif isinstance(content_block, list):
            rendered_block = ''
            for (idx, content_block_el) in enumerate(content_block):
                if (isinstance(content_block_el, RenderedComponentContent) or (isinstance(content_block_el, dict) and ('content_block_type' in content_block_el))):
                    new_content_block_id = None
                    if content_block_id:
                        new_content_block_id = f'{content_block_id}-{str(idx)}'
                    rendered_block += self.render_content_block(jinja_context, content_block_el, idx, content_block_id=new_content_block_id)
                elif render_to_markdown:
                    rendered_block += str(content_block_el)
                else:
                    rendered_block += f'<span>{str(content_block_el)}</span>'
            return rendered_block
        elif (not isinstance(content_block, dict)):
            return content_block
        content_block_type = content_block.get('content_block_type')
        if (content_block_type is None):
            return content_block
        if render_to_markdown:
            template_filename = f'markdown_{content_block_type}.j2'
        else:
            template_filename = f'{content_block_type}.j2'
        template = self._get_template(template=template_filename)
        if content_block_id:
            return template.render(jinja_context, content_block=content_block, index=index, content_block_id=content_block_id)
        else:
            return template.render(jinja_context, content_block=content_block, index=index)

    def render_dict_values(self, context, dict_, index=None, content_block_id=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        for (key, val) in dict_.items():
            if key.startswith('_'):
                continue
            dict_[key] = self.render_content_block(context, val, index, content_block_id)

    @contextfilter
    def render_bootstrap_table_data(self, context, table_data, index=None, content_block_id=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        for table_data_dict in table_data:
            self.render_dict_values(context, table_data_dict, index, content_block_id)
        return table_data

    def get_html_escaped_json_string_from_dict(self, source_dict):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return json.dumps(source_dict).replace('"', '\\"').replace('"', '&quot;')

    def attributes_dict_to_html_string(self, attributes_dict, prefix=''):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        attributes_string = ''
        if prefix:
            prefix += '-'
        for (attribute, value) in attributes_dict.items():
            attributes_string += f'{prefix}{attribute}="{value}" '
        return attributes_string

    def render_styling(self, styling):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Adds styling information suitable for an html tag.\n\n        Example styling block::\n\n            styling = {\n                "classes": ["alert", "alert-warning"],\n                "attributes": {\n                    "role": "alert",\n                    "data-toggle": "popover",\n                },\n                "styles" : {\n                    "padding" : "10px",\n                    "border-radius" : "2px",\n                }\n            }\n\n        The above block returns a string similar to::\n\n            \'class="alert alert-warning" role="alert" data-toggle="popover" style="padding: 10px; border-radius: 2px"\'\n\n        "classes", "attributes" and "styles" are all optional parameters.\n        If they aren\'t present, they simply won\'t be rendered.\n\n        Other dictionary keys are also allowed and ignored.\n        '
        class_list = styling.get('classes', None)
        if (class_list is None):
            class_str = ''
        else:
            if (type(class_list) == str):
                raise TypeError('classes must be a list, not a string.')
            class_str = f"""class="{' '.join(class_list)}" """
        attribute_dict = styling.get('attributes', None)
        if (attribute_dict is None):
            attribute_str = ''
        else:
            attribute_str = ''
            for (k, v) in attribute_dict.items():
                attribute_str += f'{k}="{v}" '
        style_dict = styling.get('styles', None)
        if (style_dict is None):
            style_str = ''
        else:
            style_str = 'style="'
            style_str += ' '.join([f'{k}:{v};' for (k, v) in style_dict.items()])
            style_str += '" '
        styling_string = pTemplate('$classes$attributes$style').substitute({'classes': class_str, 'attributes': attribute_str, 'style': style_str})
        return styling_string

    def render_styling_from_string_template(self, template):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'This method is a thin wrapper use to call `render_styling` from within jinja templates.'
        if (not isinstance(template, (dict, OrderedDict))):
            return template
        if ('styling' in template):
            return self.render_styling(template['styling'])
        else:
            return ''

    def generate_html_element_uuid(self, prefix=None):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if prefix:
            return (prefix + str(uuid4()))
        else:
            return str(uuid4())

    def render_markdown(self, markdown):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        try:
            return mistune.markdown(markdown)
        except OSError:
            return markdown

    def render_string_template(self, template):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(template, (dict, OrderedDict))):
            return template
        template['template'] = re.sub('\\${2,}', (lambda m: (m.group(0) * 2)), template.get('template', ''))
        tag = template.get('tag', 'span')
        template['template'] = template.get('template', '').replace('$PARAMETER', '$$PARAMETER')
        template['template'] = template.get('template', '').replace('\n', '<br>')
        if ('tooltip' in template):
            if template.get('styling', {}).get('classes'):
                classes = template.get('styling', {}).get('classes')
                classes.append('cooltip')
                template['styling']['classes'] = classes
            elif template.get('styling'):
                template['styling']['classes'] = ['cooltip']
            else:
                template['styling'] = {'classes': ['cooltip']}
            tooltip_content = template['tooltip']['content']
            tooltip_content.replace('\n', '<br>')
            placement = template['tooltip'].get('placement', 'top')
            base_template_string = '\n                <{tag} $styling>\n                    $template\n                    <span class={placement}>\n                        {tooltip_content}\n                    </span>\n                </{tag}>\n            '.format(placement=placement, tooltip_content=tooltip_content, tag=tag)
        else:
            base_template_string = '\n                <{tag} $styling>\n                    $template\n                </{tag}>\n            '.format(tag=tag)
        if ('styling' in template):
            params = template.get('params', {})
            if ('default' in template['styling']):
                default_parameter_styling = template['styling']['default']
                default_param_tag = default_parameter_styling.get('tag', 'span')
                base_param_template_string = '<{param_tag} $styling>$content</{param_tag}>'.format(param_tag=default_param_tag)
                for parameter in template['params'].keys():
                    if ('params' in template['styling']):
                        if (parameter in template['styling']['params']):
                            continue
                    params[parameter] = pTemplate(base_param_template_string).safe_substitute({'styling': self.render_styling(default_parameter_styling), 'content': params[parameter]})
            if ('params' in template['styling']):
                for (parameter, parameter_styling) in template['styling']['params'].items():
                    if (parameter not in params):
                        continue
                    param_tag = parameter_styling.get('tag', 'span')
                    param_template_string = '<{param_tag} $styling>$content</{param_tag}>'.format(param_tag=param_tag)
                    params[parameter] = pTemplate(param_template_string).safe_substitute({'styling': self.render_styling(parameter_styling), 'content': params[parameter]})
            string = pTemplate(pTemplate(base_template_string).safe_substitute({'template': template['template'], 'styling': self.render_styling(template.get('styling', {}))})).safe_substitute(params)
            return string
        return pTemplate(pTemplate(base_template_string).safe_substitute({'template': template.get('template', ''), 'styling': self.render_styling(template.get('styling', {}))})).safe_substitute(template.get('params', {}))

    def _validate_document(self, document) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        raise NotImplementedError

class DefaultJinjaPageView(DefaultJinjaView):
    _template = 'page.j2'

    def _validate_document(self, document) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        assert isinstance(document, RenderedDocumentContent)

class DefaultJinjaIndexPageView(DefaultJinjaPageView):
    _template = 'index_page.j2'

class DefaultJinjaSectionView(DefaultJinjaView):
    _template = 'section.j2'

    def _validate_document(self, document) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        assert isinstance(document['section'], dict)

class DefaultJinjaComponentView(DefaultJinjaView):
    _template = 'component.j2'

    def _validate_document(self, document) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        assert isinstance(document['content_block'], dict)

class DefaultMarkdownPageView(DefaultJinjaView):
    '\n    Convert a document to markdown format.\n    '

    def _validate_document(self, document: RenderedDocumentContent) -> bool:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Validate that the document is of the appropriate type at runtime.\n        '
        assert isinstance(document, RenderedDocumentContent)
    _template = 'markdown_validation_results_page.j2'

    def render(self, document, template=None, **kwargs):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Handle list as well as single document\n        '
        if isinstance(document, list):
            return [super(DefaultMarkdownPageView, self).render(document=d, template=template, **kwargs) for d in document]
        else:
            return super().render(document=document, template=template, **kwargs)

    def render_string_template(self, template: pTemplate) -> pTemplate:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Render string for markdown rendering. Bold all parameters and perform substitution.\n        Args:\n            template: python Template object\n\n        Returns:\n            Template with substituted values and all parameters bolded\n\n        '
        if (not isinstance(template, (dict, OrderedDict))):
            return template
        tag = template.get('tag', None)
        if (tag and (tag == 'hr')):
            template['template'] = '***'
        template['template'] = re.sub('\\${2,}', (lambda m: (m.group(0) * 2)), template.get('template', ''))
        base_param_template_string = '**$content**'
        template['params'] = template.get('params', {})
        if ('markdown_status_icon' in template['params']):
            return template['params']['markdown_status_icon']
        for parameter in template['params'].keys():
            if (parameter == 'html_success_icon'):
                template['params'][parameter] = ''
                continue
            elif (template['params'][parameter] == '*'):
                template['params'][parameter] = '\\*'
                continue
            template['params'][parameter] = pTemplate(base_param_template_string).safe_substitute({'content': template['params'][parameter]})
        template['template'] = template.get('template', '').replace('$PARAMETER', '$$PARAMETER')
        return pTemplate(template.get('template')).safe_substitute(template.get('params', {}))

    @contextfilter
    def render_content_block(self, jinja_context, content_block, index=None, content_block_id=None, render_to_markdown: bool=True):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Render a content block to markdown using jinja templates.\n        Args:\n            jinja_context:\n            content_block:\n            index:\n            content_block_id:\n            render_to_markdown: Default of True here instead of parent class default of False\n\n        Returns:\n\n        '
        return super().render_content_block(jinja_context=jinja_context, content_block=content_block, index=index, content_block_id=content_block_id, render_to_markdown=render_to_markdown)
