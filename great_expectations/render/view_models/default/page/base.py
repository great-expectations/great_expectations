from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape
)

from ....base import Renderer


class FullPageHtmlRenderer(Renderer):
    @classmethod
    def _validate_input(cls, expectations):
        # raise NotImplementedError
        #!!! Need to fix this
        return True

    @classmethod
    def _get_template(cls):
        env = Environment(
            loader=PackageLoader(
                'great_expectations',
                'render/view_models/default/fixtures/templates'
            ),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('page.j2')

    @classmethod
    def render(cls, input):
        raise NotImplementedError
