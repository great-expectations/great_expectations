from great_expectations.render.renderer import (
    PrescriptivePageRenderer,
    DescriptivePageRenderer,
)

def test_render_asset_notes():
    # import pypandoc
    # print(pypandoc.convert_text("*hi*", to='html', format="md"))

    result = PrescriptivePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : "*hi*"
        }
    })
    print(result)
    assert result["content"] == ["*hi*"]

    result = PrescriptivePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : ["*alpha*", "_bravo_", "charlie"]
        }
    })
    print(result)
    assert result["content"] == ["*alpha*", "_bravo_", "charlie"]

    result = PrescriptivePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "string",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    })
    print(result)
    assert result["content"] == ["*alpha*", "_bravo_", "charlie"]

    result = PrescriptivePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "markdown",
                "content": "*alpha*"
            }
        }
    })
    print(result)
    assert result["content"] == ["<p><em>alpha</em></p>\n"]

    result = PrescriptivePageRenderer._render_asset_notes({
        "meta" : {
            "notes" : {
                "format": "markdown",
                "content": ["*alpha*", "_bravo_", "charlie"]
            }
        }
    })
    print(result)
    assert result["content"] == ["<p><em>alpha</em></p>\n", "<p><em>bravo</em></p>\n", "<p>charlie</p>\n"]


def test_expectation_summary_in_render_asset_notes():
    pass