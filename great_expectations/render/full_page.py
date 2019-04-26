import os
import random

from jinja2 import Template
from jinja2 import Environment, BaseLoader, PackageLoader, select_autoescape

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
        env = Environment(
            loader=PackageLoader('great_expectations', 'render/fixtures'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        t = env.get_template('single_page_prescriptive.j2')

        results = []
        for expectation in self.expectations:
            expectation_renderer = SingleExpectationRenderer(
                expectation=expectation,
            )
            results.append(expectation_renderer.render())
        # print(results)

        rendered_page = t.render(
            **{
            "elements": results
        })
        # print(rendered_page)

        return rendered_page