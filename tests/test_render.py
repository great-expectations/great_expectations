import unittest
import json

import great_expectations as ge
from great_expectations import render
from .test_utils import assertDeepAlmostEqual


class TestPageRenderers(unittest.TestCase):

    def test_import(self):
        from great_expectations import render

    def test_prescriptive_expectation_renderer(self):
        expectations_config = json.load(
            open('tests/test_fixtures/rendering_fixtures/expectation_suite_3.json')
        )
        results = render.view_models.PrescriptiveExpectationPageRenderer().render(
            expectations_config,
        )
        assert results != None
        assert "<li> is a required field.</li>" in results
        assert '<li> must have at least <span class="param-span">0</span> unique values.</li>' in results

        with open('./test.html', 'w') as f:
            f.write(results)

    def test_descriptive_evr_renderer(self):
        rendered_page = render.view_models.DescriptiveEvrPageRenderer().render(
            json.load(
                open('tests/test_fixtures/rendering_fixtures/evr_suite_3.json')
            )["results"],
        )
        assert rendered_page != None

        # with open('./test.html', 'w') as f:
        #     f.write(rendered_page)

    def test_full_oobe_flow(self):
        df = ge.read_csv("examples/data/Titanic.csv")
        # df = ge.read_csv("examples/data/Meteorite_Landings.csv")
        df.autoinspect(ge.dataset.autoinspect.pseudo_pandas_profiling)
        # df.autoinspect(ge.dataset.autoinspect.columns_exist)
        evrs = df.validate()["results"]
        # print(json.dumps(evrs, indent=2))

        rendered_page = render.compile_to_documentation(
            evrs,
            render.view_models.DescriptiveEvrPageRenderer,
        )
        assert rendered_page != None

        # with open('./test.html', 'w') as f:
        #     f.write(rendered_page)


class TestSectionRenderers(unittest.TestCase):

    def test_render_modes(self):
        # df = ge.read_csv("examples/data/Meteorite_Landings.csv")
        # df.autoinspect(ge.dataset.autoinspect.pseudo_pandas_profiling)
        # expectations_list = df.get_expectations_config()["expectations"]

        expectations_list = json.load(
            open('tests/test_fixtures/rendering_fixtures/expectation_suite_3.json')
        )["expectations"]

        # print( json.dumps(expectations_list, indent=2) )

        # evrs = df.validate()["results"]
        # print( json.dumps(evrs, indent=2) )

        R = render.view_models.default.section.prescriptive.PrescriptiveExpectationColumnSectionRenderer
        rendered_section = R.render(
            expectations_list
        )
        assert rendered_section != None
        assert json.dumps(rendered_section)
        # print( json.dumps(rendered_section, indent=2) )

        rendered_section = R.render(
            expectations_list,
            'html'
        )
        # print(rendered_section)

        assert "<li> is a required field.</li>" in rendered_section
        # assert False


class TestSnippetRenderers(unittest.TestCase):

    def test_util_render_parameter(self):
        #!!! More tests needed here, eventually.
        assert render.snippets.util.render_parameter(
            100, "d") == '<span class="param-span">100</span>'

    def test_basics(self):
        #!!! Many more tests needed here, eventually.

        result = render.snippets.expectation_bullet_point.ExpectationBulletPointSnippetRenderer.render({
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": "x_var"}
        }, include_column_name=True)
        print(result)
        assert result == "x_var is a required field."

        result = render.snippets.expectation_bullet_point.ExpectationBulletPointSnippetRenderer.render(
            {
                "expectation_type": "expect_column_value_lengths_to_be_between",
                "kwargs": {
                    "column": "last_name",
                    "min_value": 3,
                    "max_value": 20,
                    "mostly": .95
                }
            }, include_column_name=False)
        print(result)
        assert result == ' must be between <span class="param-span">3</span> and <span class="param-span">20</span> characters long at least <span class="param-span">0.9</span>% of the time.'


class TestContentBlockRenderers(unittest.TestCase):
    result = render.snippets.evr_content_block.EvrContentBlockSnippetRenderer.render(
        {
            'success': False,
            'result': {
                'element_count': 45716,
                'missing_count': 0,
                'missing_percent': 0.0,
                'unexpected_count': 45716,
                'unexpected_percent': 1.0,
                'unexpected_percent_nonmissing': 1.0,
                'partial_unexpected_list': [
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid',
                    'Valid'
                ],
                'partial_unexpected_index_list': [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19
                ],
                'partial_unexpected_counts': [{'value': 'Valid', 'count': 45641},
                                              {'value': 'Relict', 'count': 75}]
            },
            'exception_info': {
                'raised_exception': False,
                'exception_message': None,
                'exception_traceback': None
            },
            'expectation_config': {
                'expectation_type': 'expect_column_values_to_be_in_set',
                'kwargs': {
                    'column': 'nametype',
                    'value_set': [],
                    'result_format': 'SUMMARY'
                }
            }
        },
    )
    print(json.dumps(result))

    # assert json.dumps(result) == """{"content_block_type": "graph", "content": [{"$schema": "https://vega.github.io/schema/vega-lite/v2.6.0.json", "config": {"view": {"height": 300, "width": 400}}, "datasets": {"data-cfff8a6fe8134dace707fd67405d0857": [{"count": 45641, "value": "Valid"}, {"count": 75, "value": "Relict"}]}, "height": 900, "layer": [{"data": {"name": "data-cfff8a6fe8134dace707fd67405d0857"}, "encoding": {"x": {"field": "count", "type": "quantitative"}, "y": {"field": "value", "type": "ordinal"}}, "height": 80, "mark": "bar", "width": 240}, {"data": {"name": "data-cfff8a6fe8134dace707fd67405d0857"}, "encoding": {"text": {"field": "count", "type": "quantitative"}, "x": {"field": "count", "type": "quantitative"}, "y": {"field": "value", "type": "ordinal"}}, "height": 80, "mark": {"align": "left", "baseline": "middle", "dx": 3, "type": "text"}, "width": 240}]}]}"""
    assertDeepAlmostEqual(
        result,
        {
            "content_block_type": "graph",
            "content": [{
                "$schema": "https://vega.github.io/schema/vega-lite/v2.6.0.json",
                "config": {"view": {"height": 300, "width": 400}},
                "datasets": {
                    "data-cfff8a6fe8134dace707fd67405d0857": [
                        {"count": 45641, "value": "Valid"}, {
                            "count": 75, "value": "Relict"}
                    ]},
                "height": 900,
                "layer": [{
                    "data": {"name": "data-cfff8a6fe8134dace707fd67405d0857"},
                    "encoding": {
                        "x": {"field": "count", "type": "quantitative"},
                        "y": {"field": "value", "type": "ordinal"}
                    },
                    "height": 80,
                    "mark": "bar",
                    "width": 240
                }, {
                    "data": {"name": "data-cfff8a6fe8134dace707fd67405d0857"},
                    "encoding": {
                        "text": {"field": "count", "type": "quantitative"},
                        "x": {"field": "count", "type": "quantitative"},
                        "y": {"field": "value", "type": "ordinal"}
                    },
                    "height": 80,
                    "mark": {"align": "left", "baseline": "middle", "dx": 3, "type": "text"},
                    "width": 240
                }]
            }]
        }
    )
