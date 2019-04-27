import unittest
import json

from great_expectations import render

class TestPageRenderers(unittest.TestCase):

    def test_import(self):
        from great_expectations import render

    def test_prescriptive_expectation_renderer(self):
        results = render.render(
            renderer_class=render.PrescriptiveExpectationPageRenderer,
            expectations=json.load(open('tests/test_fixtures/rendering_fixtures/expectation_suite_3.json'))["expectations"],
        )
        assert results != None

    def test_descriptive_evr_renderer(self):
        R = render.DescriptiveEvrPageRenderer(
          json.load(open('tests/test_fixtures/rendering_fixtures/evr_suite_3.json'))["results"],
        )
        rendered_page = R.render()
        assert rendered_page != None

        with open('./test.html', 'w') as f:
            f.write(rendered_page)
