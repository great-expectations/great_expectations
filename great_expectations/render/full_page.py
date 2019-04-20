import os
from jinja2 import Template

from .base import Renderer
from .single_expectation import SingleExpectationRenderer

#FullPageExpectationHtmlRenderer
#FullPageValidationResultHtmlRenderer

class FullPageHtmlRenderer(Renderer):
    def __init__(self, expectations, inspectable):
        self.expectations = expectations

    def validate_input(self, expectations):
        return True

    def render(self):
        t = Template(open(
            os.path.join(
                os.path.dirname(__file__),
                'fixtures/single_page.j2'
            )
        ).read())

        results = []
        for expectation in self.expectations:
            expectation_renderer = SingleExpectationRenderer(
                expectation=expectation,
            )
            results.append(expectation_renderer.render())
        
        print(results)

        rendered_page = t.render(**{
            "elements": results
        })
        print(rendered_page)

        return rendered_page