"""
jtbd:
* render many Expectation Suites as a full static HTML site
    navigation

* render a single Expectation Suite as a standalone HTML file
    sections

* render a single Expectation Suite as a (potentially nested) list of elements (e.g. div, p, span, JSON, jinja, markdown)
    grouping?

* render a single Expectation as a single element (e.g. div, p, span, JSON, jinja, markdown)
"""

from .full_page import (
    FullPageHtmlRenderer,
)

def render(expectations=None, input_inspectable=None, renderer_class=None, output_inspectable=None):
    renderer = renderer_class(
        expectations=expectations,
        inspectable=input_inspectable,
    )
    results = renderer.render()
    return results