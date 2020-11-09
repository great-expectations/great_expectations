import numpy as np
import pandas as pd

from great_expectations.execution_engine import SparkDFExecutionEngine


def test_add_column_row_condition(spark_session):
    from pyspark.sql import functions as F

    df = pd.DataFrame({"foo": [1, 2, 3, 3, None, 2, 3, 4, 5, 6]})
    df = spark_session.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x
                for x in record.tolist()
            )
            for record in df.to_records(index=False)
        ],
        df.columns.tolist(),
    )
    engine = SparkDFExecutionEngine(batch_data_dict={tuple(): df})
    domain_kwargs = {"column": "foo"}

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=False
    )
    assert new_domain_kwargs["row_condition"] == 'col("foo").notnull()'
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs)
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo) AND foo IS NOT NULL"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs)
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=False, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo)"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs)
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (None,), (2,), (3,), (4,), (5,), (6,)]

    # This time, our skip value *will* be nan
    df = pd.DataFrame({"foo": [1, 2, 3, 3, None, 2, 3, 4, 5, 6]})
    df = spark_session.createDataFrame(df)
    engine = SparkDFExecutionEngine(batch_data_dict={tuple(): df})

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=False, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo)"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs)
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=False
    )
    assert new_domain_kwargs["row_condition"] == 'col("foo").notnull()'
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs)
    res = df.collect()
    expected = [(1,), (2,), (3,), (3,), (np.nan,), (2,), (3,), (4,), (5,), (6,)]
    # since nan != nan by default
    assert np.allclose(res, expected, rtol=0, atol=0, equal_nan=True)
