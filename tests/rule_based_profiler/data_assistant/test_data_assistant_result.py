from typing import List, Optional

import altair as alt
import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.rule_based_profiler.data_assistant_result import (
    DataAssistantResult,
)


@pytest.mark.unit
def test_get_chart_titles():
    title_text: str = "This is the title"

    df = pd.DataFrame()

    # case where the title is passed as a string
    layer_a: alt.Chart = alt.Chart(data=df)
    layer_b: alt.Chart = alt.Chart(data=df, title=title_text)
    layered_chart: alt.LayerChart = alt.layer(layer_a, layer_b)

    assert layer_a.title == alt.Undefined
    assert layer_b.title == title_text
    assert layered_chart.title == alt.Undefined

    chart_titles: List[str] = DataAssistantResult._get_chart_titles(
        charts=[layered_chart]
    )
    assert len(chart_titles) == 1
    assert chart_titles[0] == title_text

    chart_title: Optional[str] = DataAssistantResult._get_chart_layer_title(
        layer=layered_chart
    )
    assert chart_title == title_text

    # case where the title is passed as alt.TitleParams
    layer_b: alt.Chart = alt.Chart(data=df, title=alt.TitleParams(text=title_text))
    assert layer_b.title.text == title_text

    chart_titles: List[str] = DataAssistantResult._get_chart_titles(
        charts=[layered_chart]
    )
    assert len(chart_titles) == 1
    assert chart_titles[0] == title_text

    chart_title: Optional[str] = DataAssistantResult._get_chart_layer_title(
        layer=layered_chart
    )
    assert chart_title == title_text

    # case where no title exists
    with pytest.raises(gx_exceptions.DataAssistantResultExecutionError) as e:
        DataAssistantResult._get_chart_titles(charts=[layer_a])

    assert e.value.message == "All DataAssistantResult charts must have a title."
