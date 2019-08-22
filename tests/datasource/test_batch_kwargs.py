from great_expectations.datasource.types import *


def test_batch_kwargs_id():
    kwargs1 = PandasDatasourcePathBatchKwargs(
        {
            "path": "/data/test"
        }
    )
    print(kwargs1.batch_id)

    assert kwargs1.batch_id == ""
