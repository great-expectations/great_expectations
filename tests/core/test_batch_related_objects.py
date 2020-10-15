import datetime

from great_expectations.core.batch import (
    BatchDefinition
)

def test_batch_definition_id():
    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        {
            "id": "A"
        }
    )
    print(A.id)

    B = BatchDefinition(
        "B",
        "b",
        "bbb",
        {
            "id": "B"
        }
    )
    print(B.id)

    assert A.id != B.id

def test_batch_definition_equality():
    A = BatchDefinition(
        "A",
        "a",
        "aaa",
        {
            "id": "A"
        }
    )

    B = BatchDefinition(
        "B",
        "b",
        "bbb",
        {
            "id": "B"
        }
    )

    assert A != B

    A2 = BatchDefinition(
        "A",
        "a",
        "aaa",
        {
            "id": "A"
        }
    )

    assert A == A2




