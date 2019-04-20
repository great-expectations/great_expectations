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

def render(renderer_class, expectations=None, inspectable=None):
    renderer = renderer_class(expectations=expectations, inspectable=inspectable)
    results = renderer.render()
    return results