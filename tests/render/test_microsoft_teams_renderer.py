import pytest

from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.render.renderer import MicrosoftTeamsRenderer


@pytest.mark.unit
def test_MicrosoftTeamsRenderer_render(v1_checkpoint_result: CheckpointResult):
    rendered_output = MicrosoftTeamsRenderer().render(v1_checkpoint_result)
    body = rendered_output["attachments"][0]["content"]["body"]

    first_validation_result_text_blocks = body[1]["items"][0]["text"]
    assert (
        first_validation_result_text_blocks[0]["text"] == "**Batch Validation Status:** Failure :("
    )
    assert first_validation_result_text_blocks[1]["text"] == "**Data Asset Name:** my_first_asset"
    assert (
        first_validation_result_text_blocks[2]["text"] == "**Expectation Suite Name:** my_bad_suite"
    )
    assert first_validation_result_text_blocks[4]["text"] == "**Batch ID:** my_batch"
    assert (
        first_validation_result_text_blocks[5]["text"]
        == "**Summary:** *3* of *5* expectations were met"
    )

    second_validation_result_text_blocks = body[2]["items"][0]["text"]
    assert (
        second_validation_result_text_blocks[0]["text"]
        == "**Batch Validation Status:** Success !!!"
    )
    assert (
        second_validation_result_text_blocks[1]["text"]
        == "**Data Asset Name:** __no_data_asset_name__"
    )
    assert (
        second_validation_result_text_blocks[2]["text"]
        == "**Expectation Suite Name:** my_good_suite"
    )
    assert second_validation_result_text_blocks[4]["text"] == "**Batch ID:** my_other_batch"
    assert (
        second_validation_result_text_blocks[5]["text"]
        == "**Summary:** *1* of *1* expectations were met"
    )
