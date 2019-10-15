import pytest

import os

from freezegun import freeze_time

try:
    from unittest import mock
except ImportError:
    import mock

from great_expectations.datasource.types import *


@freeze_time("1955-11-05")
def test_batch_kwargs_fingerprint():
    test_batch_kwargs = PathBatchKwargs(
        {
            "path": "/data/test.csv"
        }
    )

    #demonstrate *output* kwargs post-datasource/generator

    # When there is only a single "important" key used in batch_kwargs, the ID can prominently include it
    assert test_batch_kwargs.batch_fingerprint == BatchFingerprint(
        partition_id="19551105T000000.000000Z",
        fingerprint="path:/data/test.csv")

    test_batch_kwargs = PathBatchKwargs(
        {
            "path": "/data/test.csv",
            "partition_id": "20190101"
        }
    )

    # When partition_id is explicitly included, we can extract it and potentially still have a human readable id
    assert test_batch_kwargs.batch_fingerprint == BatchFingerprint(
        partition_id="20190101",
        fingerprint="path:/data/test.csv")

    test_batch_kwargs = PathBatchKwargs(
        {
            "path": "/data/test.csv",
            "iterator": True,
            "partition_id": "3",
            "chunksize": 2e7,
            "parse_dates": [0, 3],
            "names": ["start", "type", "quantity", "end"]
        }
    )
    # When there are multiple relevant keys we use the hash of the batch_kwargs dictionary
    assert test_batch_kwargs.batch_fingerprint == BatchFingerprint(
        partition_id="3",
        fingerprint="a5d67721928ee13317a81459818a556b")


def test_batch_kwargs_from_dict():
    test_batch_kwargs = {
            "path": "/data/test.csv",
            "partition_id": "1"
        }

    # The build_batch_fingerprint convenience method makes it possible to build a batch_fingerprint from a dict.
    # HOWEVER, using it can be difficult since the default-ignored keys may depend on a specific batch_kwargs type
    assert BatchKwargs.build_batch_fingerprint(test_batch_kwargs) == BatchFingerprint(
        partition_id="1",
        fingerprint="path:/data/test.csv")
