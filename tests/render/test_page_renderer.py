import pypandoc

from great_expectations.render.renderer import (
    ExpectationSuitePageRenderer,
    ProfilingResultsPageRenderer,
)

def test_render_asset_notes():
    # import pypandoc
    # print(pypandoc.convert_text("*hi*", to='html', format="md"))

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : "*hi*"
        }
    })
    print(result)
    assert result["content"] == ["*hi*"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : ["*alpha*", "_bravo_", "charlie"]
        }
    })
    print(result)
    assert result["content"] == ["*alpha*", "_bravo_", "charlie"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "string",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    })
    print(result)
    assert result["content"] == ["*alpha*", "_bravo_", "charlie"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "markdown",
                "content": "*alpha*"
            }
        }
    })
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result["content"] == ["<p><em>alpha</em></p>\n"]
    except OSError:
        assert result["content"] == ["*alpha*"]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "markdown",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    })
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result["content"] == ["<p><em>alpha</em></p>\n", "<p><em>bravo</em></p>\n", "<p>charlie</p>\n"]
    except OSError:
        assert result["content"] == ["*alpha*", "_bravo_", "charlie"]


def test_expectation_summary_in_render_asset_notes():
    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {},
        "expectations" : {}
    })
    print(result)
    assert result["content"] == ['This Expectation suite currently contains 0 total Expectations across 0 columns.']

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "markdown",
                "content": ["hi"]
            }
        },
        "expectations" : {}
    })
    print(result)
    
    try:
        pypandoc.convert_text("*test*", format='md', to="html")
        assert result["content"] == [
            'This Expectation suite currently contains 0 total Expectations across 0 columns.',
            '<p>hi</p>\n',
        ]
    except OSError:
        assert result["content"] == [
            'This Expectation suite currently contains 0 total Expectations across 0 columns.',
            'hi',
        ]

    result = ExpectationSuitePageRenderer._render_asset_notes({
        "meta" : {},
        "expectations" : [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": { "min_value": 0, "max_value": None, }
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": { "column": "x", }
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": { "column": "y", }
            },
        ]
    })
    print(result)
    assert result["content"][0] == 'This Expectation suite currently contains 3 total Expectations across 2 columns.'


def test_ProfilingResultsPageRenderer(titanic_profiled_evrs_1):
    document = ProfilingResultsPageRenderer().render(titanic_profiled_evrs_1)
    print(document)
    # assert document == 0

