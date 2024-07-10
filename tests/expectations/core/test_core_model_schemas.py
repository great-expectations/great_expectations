import json
from pathlib import Path

import pytest

from great_expectations.expectations import core
from great_expectations.expectations.core import schemas
from great_expectations.expectations.expectation import MetaExpectation


@pytest.mark.unit
def test_all_core_model_schemas_are_serializable():
    all_models = [
        expectation
        for expectation in core.__dict__.values()
        if isinstance(expectation, MetaExpectation)
    ]
    # are they still there?
    assert len(all_models) > 50
    for model in all_models:
        model.schema_json()


@pytest.mark.unit
def test_schemas_updated():
    all_models = {
        cls_name: expectation
        for cls_name, expectation in core.__dict__.items()
        if isinstance(expectation, MetaExpectation)
    }
    schema_file_paths = Path(schemas.__file__).parent.glob("*.json")
    all_schemas = {
        file_path.stem: json.loads(file_path.read_bytes()) for file_path in schema_file_paths
    }
    for cls_name, schema in all_schemas.items():
        assert (
            schema == all_models[cls_name].schema()
        ), "json schemas not updated, run `invoke schemas --sync`"
