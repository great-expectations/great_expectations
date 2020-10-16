from great_expectations.execution_environment.data_connector.partitioner.partition import (
    Partition,
)


def test_partition():
    test_partition = Partition(
        name="test",
        data_asset_name="fake",
        definition={"name": "hello"},
        data_reference="nowhere",
    )
    # properties
    assert test_partition.name == "test"
    assert test_partition.data_asset_name == "fake"
    assert test_partition.definition == {"name": "hello"}
    assert test_partition.data_reference == "nowhere"

    assert str(test_partition) == str(
        {
            "name": "test",
            "data_asset_name": "fake",
            "definition": {"name": "hello"},
            "data_reference": "nowhere",
        }
    )
    # test __eq__()
    test_partition1 = Partition(
        name="test",
        data_asset_name="fake",
        definition={"name": "hello"},
        data_reference="nowhere",
    )
    test_partition2 = Partition(
        name="test",
        data_asset_name="fake",
        definition={"name": "hello"},
        data_reference="nowhere",
    )
    test_partition3 = Partition(
        name="i_am_different",
        data_asset_name="fake",
        definition={"name": "hello"},
        data_reference="nowhere",
    )
    assert test_partition1 == test_partition2
    assert test_partition1 != test_partition3

    # test __hash__()
    assert test_partition.__hash__() == test_partition.__hash__()
    assert test_partition1.__hash__() == test_partition2.__hash__()
    assert test_partition1.__hash__() != test_partition3.__hash__()
