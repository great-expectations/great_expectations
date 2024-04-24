import pytest

from great_expectations.checkpoint.checkpoint import CheckpointResult
from great_expectations.render.renderer import OpsgenieRenderer


@pytest.mark.unit
def test_OpsgenieRenderer_render(v1_checkpoint_result: CheckpointResult):
    # Act
    renderer = OpsgenieRenderer()
    raw_output = renderer.render(checkpoint_result=v1_checkpoint_result)
    parts = raw_output.split("\n")

    # Assert
    header = parts.pop(0)  # Separately evaluate header due to dynamic content
    assert "Checkpoint:" in header and "Run ID:" in header
    assert parts == [
        "Status: Failed ❌",
        "",
        "Batch Validation Status: Failed ❌",
        "Expectation Suite Name: my_bad_suite",
        "Data Asset Name: my_first_asset",
        "Run ID: __no_run_id__",
        "Batch ID: my_batch",
        "Summary: 3 of 5 expectations were met",
        "",
        "Batch Validation Status: Success 🎉",
        "Expectation Suite Name: my_good_suite",
        "Data Asset Name: __no_data_asset_name__",
        "Run ID: my_run_id",
        "Batch ID: my_other_batch",
        "Summary: 1 of 1 expectations were met",
    ]
