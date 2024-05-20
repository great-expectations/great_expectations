from great_expectations.expectations import core
from great_expectations.expectations.expectation import MetaExpectation


def test_all_core_models_are_serializable():
    all_models = [
        expectation
        for expectation in core.__dict__.values()
        if isinstance(expectation, MetaExpectation)
    ]
    # are they still there?
    assert len(all_models) > 50
    for model in all_models:
        model.schema_json()


def test_title_serialization():
    expectation = core.ExpectColumnValuesToNotBeNull
    title = expectation.schema()["title"]
    assert title == "Expect Column Values To Not Be Null"
