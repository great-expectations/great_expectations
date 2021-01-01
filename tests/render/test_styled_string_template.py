import json

import pytest

import great_expectations as ge
import great_expectations.render as render
from great_expectations.render.renderer import (
    ExpectationSuiteColumnSectionRenderer,
    ExpectationSuitePageRenderer,
    ProfilingResultsColumnSectionRenderer,
    ProfilingResultsPageRenderer,
)
from great_expectations.render.view import DefaultJinjaPageView


def test_render_template():
    assert (
        DefaultJinjaPageView()
        .render_string_template(
            {
                "template": "It was the $first_adj of times; it was the $second_adj of times.",
                "params": {
                    "first_adj": "best",
                    "second_adj": "worst",
                },
            }
        )
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
        == "<span>It was the best of times; it was the worst of times.</span>".replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )

    assert (
        DefaultJinjaPageView()
        .render_string_template(
            {
                "template": "It was the $first_adj of times; it was the $second_adj of times.",
                "params": {
                    "first_adj": "best",
                    "second_adj": "worst",
                },
                "styling": {
                    "default": {
                        "classes": ["badge", "badge-warning"],
                    }
                },
            }
        )
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
        == '<span>It was the <span class="badge badge-warning" >best</span> of times; it was the <span class="badge badge-warning" >worst</span> of times.</span>'.replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )

    assert (
        DefaultJinjaPageView()
        .render_string_template(
            {
                "template": "It was the $first_adj of times; it was the $second_adj of times.",
                "params": {
                    "first_adj": "best",
                    "second_adj": "worst",
                },
                "styling": {
                    "default": {
                        "classes": ["badge", "badge-warning"],
                    },
                    "params": {
                        "first_adj": {
                            "classes": ["badge-error"],
                        }
                    },
                },
            }
        )
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
        == '<span>It was the <span class="badge-error" >best</span> of times; it was the <span class="badge badge-warning" >worst</span> of times.</span>'.replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )

    assert (
        DefaultJinjaPageView()
        .render_string_template(
            {
                "template": "It was the $first_adj of times; it was the $second_adj of times.",
                "params": {
                    "first_adj": "best",
                    "second_adj": "worst",
                },
                "styling": {
                    "params": {
                        "first_adj": {
                            "classes": ["badge", "badge-warning"],
                        }
                    }
                },
            }
        )
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
        == '<span>It was the <span class="badge badge-warning" >best</span> of times; it was the worst of times.</span>'.replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )

    assert (
        DefaultJinjaPageView()
        .render_string_template(
            {
                "template": "It was the $first_adj of times; it was the $second_adj of times.",
                "params": {
                    "first_adj": "best",
                    "second_adj": "worst",
                },
                "styling": {
                    "params": {
                        "first_adj": {
                            "classes": ["badge", "badge-warning"],
                            "attributes": {"role": "alert"},
                            "styles": {"padding": "5px"},
                        }
                    }
                },
            }
        )
        .replace(" ", "")
        .replace("\t", "")
        .replace("\n", "")
        == '<span>It was the <span class="badge badge-warning" role="alert" style="padding:5px;" >best</span> of times; it was the worst of times.</span>'.replace(
            " ", ""
        )
        .replace("\t", "")
        .replace("\n", "")
    )


def test_render_template_with_extra_dollar_signs_in_template():
    result = DefaultJinjaPageView().render_string_template(
        {
            "template": "It was the $first_adj of times; it was the $second_adj of times. Blahhh$hhhh. $Bloooop. "
            "Bleep$.",
            "params": {
                "first_adj": "best",
                "second_adj": "worst",
            },
        }
    )
    assert result.replace(" ", "").replace("\t", "").replace(
        "\n", ""
    ) == "<span>It was the best of times; it was the worst of times. Blahhh$hhhh. $Bloooop. Bleep$.</span>".replace(
        " ", ""
    ).replace(
        "\t", ""
    ).replace(
        "\n", ""
    )

    result = DefaultJinjaPageView().render_string_template(
        {
            "template": "It was the $first_adj of times; it was the $second_adj of times. Blahhh$$$hhhh. $$Bloooop. Bleep$$$$$.",
            "params": {
                "first_adj": "best",
                "second_adj": "worst",
            },
        }
    )
    assert result.replace(" ", "").replace("\t", "").replace(
        "\n", ""
    ) == "<span>It was the best of times; it was the worst of times. Blahhh$$$hhhh. $$Bloooop. Bleep$$$$$.</span>".replace(
        " ", ""
    ).replace(
        "\t", ""
    ).replace(
        "\n", ""
    )


def test_render_template_with_extra_dollar_signs_in_param_values():
    result = DefaultJinjaPageView().render_string_template(
        {
            "template": "It was the $first_adj of times; it was the $second_adj of times. Blahhh$hhhh. $Bloooop. "
            "Bleep$.",
            "params": {
                "first_adj": "$best$",
                "second_adj": "$$worst$",
            },
        }
    )
    assert result.replace(" ", "").replace("\t", "").replace(
        "\n", ""
    ) == "<span>It was the $best$ of times; it was the $$worst$ of times. Blahhh$hhhh. $Bloooop. Bleep$.</span>".replace(
        " ", ""
    ).replace(
        "\t", ""
    ).replace(
        "\n", ""
    )
