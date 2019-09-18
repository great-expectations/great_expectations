import pytest

import sqlalchemy.dialects.sqlite as sqlite_dialect

from great_expectations.dataset import SqlAlchemyDataset
from great_expectations.dataset.util import build_continuous_partition_object, is_valid_continuous_partition_object


def test_build_continuous_partition_object(numeric_high_card_dataset):
    # NOTE: this test will fail if sqlite is the driver because it lacks quantile support
    # skip in that case
    if isinstance(numeric_high_card_dataset, SqlAlchemyDataset) and \
            isinstance(numeric_high_card_dataset.engine.dialect, sqlite_dialect.dialect):
        pytest.skip()

    partition = build_continuous_partition_object(
        dataset=numeric_high_card_dataset,
        column="norm_0_1",
        bins='auto',
        n_bins=1
    )
    assert len(partition["weights"]) == 9
    assert is_valid_continuous_partition_object(partition)

    partition = build_continuous_partition_object(
        dataset=numeric_high_card_dataset,
        column="norm_0_1",
        bins='ntile',
        n_bins=7
    )
    assert len(partition["weights"]) == 7
    assert is_valid_continuous_partition_object(partition)

    partition = build_continuous_partition_object(
        dataset=numeric_high_card_dataset,
        column="norm_0_1",
        bins='uniform',
        n_bins=5
    )
    assert len(partition["weights"]) == 5
    assert is_valid_continuous_partition_object(partition)
