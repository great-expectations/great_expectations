import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe


@pytest.mark.unit
def test_observed_value_of_zero_kl_divergence_renders_as_zero():
    """A KL divergence of exactly zero is an observed value, not a missing one."""
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas(name="pandas")
    asset = datasource.add_dataframe_asset(name="dataframe")
    batch_definition = asset.add_batch_definition_whole_dataframe(name="batch_definition")
    dataframe = pd.DataFrame({"x": [1, 2, 2, 4]})
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

    expectation = gxe.ExpectColumnKLDivergenceToBeLessThan(
        column="x",
        partition_object={"values": [1, 2, 4], "weights": [0.25, 0.5, 0.25]},
        threshold=0.1,
    )

    result = batch.validate(expectation)

    assert result.success
    assert result.result["observed_value"] == 0.0

    result.render()
    assert len(result.rendered_content) == 1

    rendered_content = result.rendered_content[0].to_json_dict()
    assert rendered_content["name"] == "atomic.diagnostic.observed_value"
    observed_value = rendered_content["value"]["header"]["value"]["params"]["observed_value"][
        "value"
    ]
    assert observed_value == "0"
