import pytest
from great_expectations_v1.expectations import core
from great_expectations_v1.expectations.expectation import Expectation, MetaExpectation


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


class ExpectJSONToBeFoo(Expectation): ...


class Expect123ToBeNumbers(Expectation): ...


class Expect_ToBeAToken(Expectation): ...


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation", "expected_title"),
    (
        (core.ExpectColumnValuesToNotBeNull, "Expect Column Values To Not Be Null"),
        (ExpectJSONToBeFoo, "Expect JSON To Be Foo"),
        (Expect123ToBeNumbers, "Expect 123 To Be Numbers"),
        (Expect_ToBeAToken, "Expect _ To Be A Token"),
    ),
)
def test_schema_title(expectation, expected_title):
    actual_title = expectation.schema()["title"]
    assert actual_title == expected_title
