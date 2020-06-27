import numpy as np
import pytest

from great_expectations.dataset import SqlAlchemyDataset
from great_expectations.dataset.util import (
    build_continuous_partition_object,
    is_valid_continuous_partition_object,
)


def test_build_continuous_partition_object(
    numeric_high_card_dataset, numeric_high_card_dict
):
    # NOTE: this test will fail if sqlite is the driver because it lacks quantile support
    # skip in that case
    try:
        import sqlalchemy.dialects.sqlite as sqlite_dialect

        if isinstance(numeric_high_card_dataset, SqlAlchemyDataset) and isinstance(
            numeric_high_card_dataset.engine.dialect, sqlite_dialect.dialect
        ):
            pytest.skip()
    except ImportError:
        pytest.skip("sqlite is not available")

    n = len(numeric_high_card_dict["norm_0_1"])
    weights, bin_edges = np.histogram(numeric_high_card_dict["norm_0_1"], bins="auto")

    partition = build_continuous_partition_object(
        dataset=numeric_high_card_dataset, column="norm_0_1", bins="auto", n_bins=1
    )

    assert len(partition["weights"]) == 27
    assert np.allclose(partition["weights"], weights / n)
    assert np.allclose(partition["bins"], bin_edges)
    assert is_valid_continuous_partition_object(partition)

    partition = build_continuous_partition_object(
        dataset=numeric_high_card_dataset, column="norm_0_1", bins="ntile", n_bins=7
    )
    assert len(partition["weights"]) == 7
    assert is_valid_continuous_partition_object(partition)

    weights, bin_edges = np.histogram(numeric_high_card_dict["norm_0_1"], bins=5)

    partition = build_continuous_partition_object(
        dataset=numeric_high_card_dataset, column="norm_0_1", bins="uniform", n_bins=5
    )
    assert len(partition["weights"]) == 5
    assert np.allclose(partition["weights"], weights / n)
    assert np.allclose(partition["bins"], bin_edges)
    assert is_valid_continuous_partition_object(partition)
