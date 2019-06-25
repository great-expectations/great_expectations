import json
from string import Template as pTemplate

from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape
)


def render_template(template):
    return pTemplate(template["template"]).substitute(template["params"])


class NoOpTemplate(object):
    @classmethod
    def render(cls, document):
        return document


class PrettyPrintTemplate(object):
    @classmethod
    def render(cls, document, indent=2):
        print(json.dumps(document, indent=indent))


class View(object):
    """Defines a method for converting a document to human-consumable form"""

    _template = NoOpTemplate

    @classmethod
    def render(cls, document, template=None):
        if template is None:
            template = cls._template

        t = cls._get_template(template)
        return t.render(document)

    @classmethod
    def _get_template(cls, template):
        if template is None:
            return NoOpTemplate

        env = Environment(
            loader=PackageLoader(
                'great_expectations',
                'render/view/templates'
            ),
            autoescape=select_autoescape(['html', 'xml'])
        )
        env.filters['render_template'] = render_template
        return env.get_template(template)


class EVRView(View):
    pass


class ExpectationsView(View):
    pass


class DataProfileView(View):
    pass


class ColumnHeaderView(View):
    _template = "header.j2"


class ValueListView(View):
    _template = "value_list.j2"


class ColumnSectionView(View):
    _template = "sections.j2"


class PageView(View):
    _template = "page.j2"


# class DescriptivePageView(PageView):
#     pass
