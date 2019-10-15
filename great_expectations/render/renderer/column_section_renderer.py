import json
from builtins import str  # PY2 compatibility
import re

import altair as alt
import pandas as pd

from .renderer import Renderer
from great_expectations.util import load_class
from .content_block import ExceptionListContentBlockRenderer

from ..types import RenderedSectionContent

from ..types import (
    RenderedComponentContent,
)


def convert_to_string_and_escape(var):
    return re.sub(r"\$", r"$$", str(var))


class ColumnSectionRenderer(Renderer):
    @classmethod
    def _get_column_name(cls, ge_object):
        # This is broken out for ease of locating future validation here
        if isinstance(ge_object, list):
            candidate_object = ge_object[0]
        else:
            candidate_object = ge_object
        try:
            if "kwargs" in candidate_object:
                # This is an expectation
                return candidate_object["kwargs"]["column"]
            elif "expectation_config" in candidate_object:
                # This is a validation
                return candidate_object["expectation_config"]["kwargs"]["column"]
            else:
                raise ValueError(
                    "Provide a column section renderer an expectation, list of expectations, evr, or list of evrs.")
        except KeyError:
            return None


class ProfilingResultsColumnSectionRenderer(ColumnSectionRenderer):

    def __init__(self, overview_table_renderer=None):
        if overview_table_renderer is None:
            overview_table_renderer = {
                "class_name": "ProfilingOverviewTableContentBlockRenderer"
            }
        self._overview_table_renderer = load_class(
            class_name=overview_table_renderer.get("class_name"),
            module_name=overview_table_renderer.get("module_name", "great_expectations.render.renderer.content_block")
        )

    #Note: Seems awkward to pass section_name and column_type into this renderer.
    #Can't we figure that out internally?
    def render(self, evrs, section_name=None, column_type=None):
        if section_name is None:
            column = self._get_column_name(evrs)
        else:
            column = section_name

        content_blocks = []

        content_blocks.append(self._render_header(evrs, column_type))
        # content_blocks.append(cls._render_column_type(evrs))
        content_blocks.append(self._render_overview_table(evrs))
        content_blocks.append(self._render_quantile_table(evrs))
        content_blocks.append(self._render_stats_table(evrs))
        content_blocks.append(self._render_histogram(evrs))
        content_blocks.append(self._render_values_set(evrs))
        content_blocks.append(self._render_bar_chart_table(evrs))

        # content_blocks.append(cls._render_statistics(evrs))
        # content_blocks.append(cls._render_common_values(evrs))
        # content_blocks.append(cls._render_extreme_values(evrs))
        # content_blocks.append(cls._render_frequency(evrs))
        # content_blocks.append(cls._render_composition(evrs))
        # content_blocks.append(cls._render_expectation_types(evrs))
        # content_blocks.append(cls._render_unrecognized(evrs))

        content_blocks.append(self._render_failed(evrs))
        # NOTE : Some render* functions return None so we filter them out
        populated_content_blocks = list(filter(None, content_blocks))

        return RenderedSectionContent(**{
            "section_name": column,
            "content_blocks": populated_content_blocks,
        })

    @classmethod
    def _render_header(cls, evrs, column_type=None):
        # NOTE: This logic is brittle
        try:
            column_name = evrs[0]["expectation_config"]["kwargs"]["column"]
        except KeyError:
            column_name = "Table-level expectations"

        return RenderedComponentContent(**{
            "content_block_type": "header",
            "header": {
                    "template": convert_to_string_and_escape(column_name),
                    "tooltip": {
                        "content": "expect_column_to_exist",
                        "placement": "top"
                    },
                },
            "subheader": {
                    "template": "Type: {column_type}".format(column_type=column_type),
                    "tooltip": {
                      "content": "expect_column_values_to_be_of_type <br>expect_column_values_to_be_in_type_list",
                    },
                },
            # {
            #     "template": column_type,
            # },
            "styling": {
                "classes": ["col-12"],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        })

    @classmethod
    def _render_expectation_types(cls, evrs, content_blocks):
        # NOTE: The evr-fetching function is an kinda similar to the code other_section_
        # renderer.ProfilingResultsOverviewSectionRenderer._render_expectation_types

        # type_counts = defaultdict(int)

        # for evr in evrs:
        #     type_counts[evr["expectation_config"]["expectation_type"]] += 1

        # bullet_list = sorted(type_counts.items(), key=lambda kv: -1*kv[1])

        bullet_list = [{
            "content_block_type": "string_template",
            "string_template": {
                "template": "$expectation_type $is_passing",
                "params": {
                    "expectation_type": evr["expectation_config"]["expectation_type"],
                    "is_passing": str(evr["success"]),
                },
                "styling": {
                    "classes": ["list-group-item", "d-flex", "justify-content-between", "align-items-center"],
                    "params": {
                        "is_passing": {
                            "classes": ["badge", "badge-secondary", "badge-pill"],
                        }
                    },
                }
            }
        } for evr in evrs]

        content_blocks.append(RenderedComponentContent(**{
            "content_block_type": "bullet_list",
            "header": 'Expectation types <span class="mr-3 triangle"></span>',
            "bullet_list": bullet_list,
            "styling": {
                "classes": ["col-12"],
                "styles": {
                    "margin-top": "20px"
                },
                "header": {
                    # "classes": ["alert", "alert-secondary"],
                    "classes": ["collapsed"],
                    "attributes": {
                        "data-toggle": "collapse",
                        "href": "#{{content_block_id}}-body",
                        "role": "button",
                        "aria-expanded": "true",
                        "aria-controls": "collapseExample",
                    },
                    "styles": {
                        "cursor": "pointer",
                    }
                },
                "body": {
                    "classes": ["list-group", "collapse"],
                },
            },
        }))

    def _render_overview_table(self, evrs):
        unique_n = self._find_evr_by_type(
            evrs,
            "expect_column_unique_value_count_to_be_between"
        )
        unique_proportion = self._find_evr_by_type(
            evrs,
            "expect_column_proportion_of_unique_values_to_be_between"
        )
        null_evr = self._find_evr_by_type(
            evrs,
            "expect_column_values_to_not_be_null"
        )
        evrs = [evr for evr in [unique_n, unique_proportion, null_evr] if (evr is not None and "result" in evr)]

        if len(evrs) > 0:
            new_content_block = self._overview_table_renderer.render(evrs)
            new_content_block["header"] = "Properties"
            new_content_block["styling"] = {
                "classes": ["col-4", ],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm", "table-unbordered"],
                    "styles": {
                        "width": "100%"
                    },
                }

            }
            return new_content_block

    @classmethod
    def _render_quantile_table(cls, evrs):
        table_rows = []

        quantile_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_quantile_values_to_be_between"
        )

        if not quantile_evr or "result" not in quantile_evr:
            return

        quantiles = quantile_evr["result"]["observed_value"]["quantiles"]
        quantile_ranges = quantile_evr["result"]["observed_value"]["values"]

        quantile_strings = {
            .25: "Q1",
            .75: "Q3",
            .50: "Median"
        }
        
        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile)
            table_rows.append([
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": quantile_string if quantile_string else "{:3.2f}".format(quantile),
                        "tooltip": {
                            "content": "expect_column_quantile_values_to_be_between \n expect_column_median_to_be_between" if quantile == 0.50 else "expect_column_quantile_values_to_be_between"
                        }
                    }
                },
                quantile_ranges[idx],
            ])

        return RenderedComponentContent(**{
            "content_block_type": "table",
            "header": "Quantiles",
            "table": table_rows,
            "styling": {
                "classes": ["col-4"],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm", "table-unbordered"],
                }
            },
        })

    @classmethod
    def _render_stats_table(cls, evrs):
        table_rows = []

        mean_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_mean_to_be_between"
        )

        if not mean_evr or "result" not in mean_evr:
            return

        mean_value = "{:.2f}".format(
            mean_evr['result']['observed_value']) if mean_evr else None
        if mean_value:
            table_rows.append([
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Mean",
                        "tooltip": {
                            "content": "expect_column_mean_to_be_between"
                        }
                    }
                },
                mean_value
            ])

        min_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_min_to_be_between"
        )
        min_value = "{:.2f}".format(
            min_evr['result']['observed_value']) if min_evr else None
        if min_value:
            table_rows.append([
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Minimum",
                        "tooltip": {
                            "content": "expect_column_min_to_be_between"
                        }
                    }
                },
                min_value,
            ])

        max_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_max_to_be_between"
        )
        max_value = "{:.2f}".format(
            max_evr['result']['observed_value']) if max_evr else None
        if max_value:
            table_rows.append([
                {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Maximum",
                        "tooltip": {
                            "content": "expect_column_max_to_be_between"
                        }
                    }
                },
                max_value
            ])

        if len(table_rows) > 0:
            return RenderedComponentContent(**{
                "content_block_type": "table",
                "header": "Statistics",
                "table": table_rows,
                "styling": {
                    "classes": ["col-4"],
                    "styles": {
                        "margin-top": "20px"
                    },
                    "body": {
                        "classes": ["table", "table-sm", "table-unbordered"],
                    }
                },
            })
        else:
            return

    @classmethod
    def _render_values_set(cls, evrs):
        set_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_values_to_be_in_set"
        )

        if not set_evr or "result" not in set_evr:
            return

        if set_evr and "partial_unexpected_counts" in set_evr["result"]:
            partial_unexpected_counts = set_evr["result"]["partial_unexpected_counts"]
            values = [str(v["value"]) for v in partial_unexpected_counts]
        elif set_evr and "partial_unexpected_list" in set_evr["result"]:
            values = [str(item) for item in set_evr["result"]["partial_unexpected_list"]]
        else:
            return

        if len(" ".join(values)) > 100:
            classes = ["col-12"]
        else:
            classes = ["col-4"]

        if any(len(value) > 80 for value in values):
            content_block_type = "bullet_list"
        else:
            content_block_type = "value_list"

        new_block = RenderedComponentContent(**{
            "content_block_type": content_block_type,
            "header":
                {
                    "template": "Example Values",
                    "tooltip": {
                        "content": "expect_column_values_to_be_in_set"
                    }
                },
            content_block_type: [{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$value",
                    "params": {
                        "value": value
                    },
                    "styling": {
                        "default": {
                            "classes": ["badge", "badge-info"] if content_block_type == "value_list" else [],
                            "styles": {
                                "word-break": "break-all"
                            }
                        },
                    }
                }
            } for value in values],
            "styling": {
                "classes": classes,
                "styles": {
                    "margin-top": "20px",
                }
            }
        })

        return new_block

    @classmethod
    def _render_histogram(cls, evrs):
        # NOTE: This code is very brittle
        kl_divergence_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_kl_divergence_to_be_less_than"
        )
        # print(json.dumps(kl_divergence_evr, indent=2))
        if not kl_divergence_evr or "result" not in kl_divergence_evr or "details" not in kl_divergence_evr.get("result", {}):
            return

        bins = kl_divergence_evr["result"]["details"]["observed_partition"]["bins"]
        # bin_medians = [round((v+bins[i+1])/2, 1)
        #                for i, v in enumerate(bins[:-1])]
        # bin_medians = [(round(bins[i], 1), round(bins[i+1], 1)) for i, v in enumerate(bins[:-1])]
        bins_x1 = [round(value, 1) for value in bins[:-1]]
        bins_x2 = [round(value, 1) for value in bins[1:]]
        weights = kl_divergence_evr["result"]["details"]["observed_partition"]["weights"]

        df = pd.DataFrame({
            "bin_min": bins_x1,
            "bin_max": bins_x2,
            "weights": weights,
        })
        df.weights *= 100

        if len(weights) <= 10:
            height = 200
            width = 200
            col_width = 4
        else:
            height = 300
            width = 300
            col_width = 6

        bars = alt.Chart(df).mark_bar().encode(
            x='bin_min:O',
            x2='bin_max:O',
            y="weights:Q"
        ).properties(width=width, height=height, autosize="fit")

        chart = bars.to_json()

        return RenderedComponentContent(**{
            "content_block_type": "graph",
            "header":
                {
                    "template": "Histogram",
                    "tooltip": {
                        "content": "expect_column_kl_divergence_to_be_less_than"
                    }
                },
            "graph": chart,
            "styling": {
                "classes": ["col-" + str(col_width)],
                "styles": {
                    "margin-top": "20px",
                }
            }
        })

    @classmethod
    def _render_bar_chart_table(cls, evrs):
        distinct_values_set_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_distinct_values_to_be_in_set"
        )
        # print(json.dumps(kl_divergence_evr, indent=2))
        if not distinct_values_set_evr or "result" not in distinct_values_set_evr:
            return

        value_count_dicts = distinct_values_set_evr['result']['details']['value_counts']
        values = [value_count_dict['value']
                  for value_count_dict in value_count_dicts]
        counts = [value_count_dict['count']
                  for value_count_dict in value_count_dicts]

        df = pd.DataFrame({
            "value": values,
            "count": counts,
        })

        if len(values) <= 10:
            height = 200
            width = 200
            col_width = 4
        else:
            height = 300
            width = 300
            col_width = 6

        bars = alt.Chart(df).mark_bar(size=20).encode(
            y='count:Q',
            x="value:O"
        ).properties(height=height, width=width, autosize="fit")

        chart = bars.to_json()

        new_block = RenderedComponentContent(**{
            "content_block_type": "graph",
            "header":
                {
                    "template": "Value Counts",
                    "tooltip": {
                        "content": "expect_column_distinct_values_to_be_in_set"
                    }
                },
            "graph": chart,
            "styling": {
                "classes": ["col-" + str(col_width)],
                "styles": {
                    "margin-top": "20px",
                }
            }
        })

        return new_block

    @classmethod
    def _render_failed(cls, evrs):
        return ExceptionListContentBlockRenderer.render(evrs, include_column_name=False)

    @classmethod
    def _render_unrecognized(cls, evrs, content_blocks):
        unrendered_blocks = []
        new_block = None
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] not in [
                "expect_column_to_exist",
                "expect_column_values_to_be_of_type",
                "expect_column_values_to_be_in_set",
                "expect_column_unique_value_count_to_be_between",
                "expect_column_proportion_of_unique_values_to_be_between",
                "expect_column_values_to_not_be_null",
                "expect_column_max_to_be_between",
                "expect_column_mean_to_be_between",
                "expect_column_min_to_be_between"
            ]:
                new_block = RenderedComponentContent(**{
                    "content_block_type": "text",
                    "content": []
                })
                new_block["content"].append("""
    <div class="alert alert-primary" role="alert">
        Warning! Unrendered EVR:<br/>
    <pre>"""+json.dumps(evr, indent=2)+"""</pre>
    </div>
                """)

        if new_block is not None:
            unrendered_blocks.append(new_block)

        # print(unrendered_blocks)
        content_blocks += unrendered_blocks


class ValidationResultsColumnSectionRenderer(ColumnSectionRenderer):

    def __init__(self, table_renderer=None):
        if table_renderer is None:
            table_renderer = {
                "class_name": "ValidationResultsTableContentBlockRenderer"
            }
        self._table_renderer = load_class(
            class_name=table_renderer.get("class_name"),
            module_name=table_renderer.get("module_name", "great_expectations.render.renderer.content_block")
        )

    @classmethod
    def _render_header(cls, validation_results):
        column = cls._get_column_name(validation_results)
        
        if not column:
            column = "Table-Level Expectations"
        
        new_block = RenderedComponentContent(**{
            "content_block_type": "header",
            "header": convert_to_string_and_escape(column),
            "styling": {
                "classes": ["col-12"],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        })
        
        return validation_results, new_block
    
    def _render_table(self, validation_results):
        new_block = self._table_renderer.render(
            validation_results,
            include_column_name=False
        )
        
        return [], new_block
    
    def render(self, validation_results={}):
        column = self._get_column_name(validation_results)
        content_blocks = []
        remaining_evrs, content_block = self._render_header(validation_results)
        content_blocks.append(content_block)
        remaining_evrs, content_block = self._render_table(remaining_evrs)
        content_blocks.append(content_block)

        return RenderedSectionContent(**{
            "section_name": column,
            "content_blocks": content_blocks
        })


class ExpectationSuiteColumnSectionRenderer(ColumnSectionRenderer):

    def __init__(self, bullet_list_renderer=None):
        if bullet_list_renderer is None:
            bullet_list_renderer = {
                "class_name": "ExpectationSuiteBulletListContentBlockRenderer"
            }
        self._bullet_list_renderer = load_class(
            class_name=bullet_list_renderer.get("class_name"),
            module_name=bullet_list_renderer.get("module_name", "great_expectations.render.renderer.content_block")
        )

    @classmethod
    def _render_header(cls, expectations):
        column = cls._get_column_name(expectations)

        new_block = RenderedComponentContent(**{
            "content_block_type": "header",
            "header": convert_to_string_and_escape(column),
            "styling": {
                "classes": ["col-12"],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        })

        return expectations, new_block

    def _render_bullet_list(self, expectations):

        new_block = self._bullet_list_renderer.render(
            expectations,
            include_column_name=False,
        )

        return [], new_block

    def render(self, expectations={}):
        column = self._get_column_name(expectations)

        content_blocks = []
        remaining_expectations, header_block = self._render_header(expectations)
        content_blocks.append(header_block)
        # remaining_expectations, content_blocks = cls._render_column_type(
        # remaining_expectations, content_blocks)
        remaining_expectations, bullet_block = self._render_bullet_list(remaining_expectations)
        content_blocks.append(bullet_block)

        # NOTE : Some render* functions return None so we filter them out
        populated_content_blocks = list(filter(None, content_blocks))
        return RenderedSectionContent(**{
            "section_name": column,
            "content_blocks": populated_content_blocks
        })
