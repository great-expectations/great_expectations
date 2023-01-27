import os

import dataprofiler as dp
import pandas as pd

# noinspection PyUnresolvedReferences
import contrib.capitalone_dataprofiler_expectations.capitalone_dataprofiler_expectations.metrics.data_profiler_metrics
from great_expectations.self_check.util import build_pandas_engine
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric

test_root_path = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
)


def test_data_profiler_column_profile_report_metric_pd():
    engine = build_pandas_engine(
        pd.DataFrame(
            {
                "VendorID": [
                    "Ada Lovelace",
                    "Alan Kay",
                    "Donald Knuth",
                    "Edsger Dijkstra",
                    "Guido van Rossum",
                    "John McCarthy",
                    "Marvin Minsky",
                    "Ray Ozzie",
                ]
            }
        )
    )

    profile_path = os.path.join(
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="data_profiler.column_profile_report",
        metric_domain_kwargs={"column": "VendorID"},
        metric_value_kwargs={
            "profile_path": profile_path,
        },
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    profile = dp.Profiler.load(profile_path)
    assert (
        results[desired_metric.id]["column_name"]
        == profile.report()["data_stats"][0]["column_name"]
    )
    assert (
        results[desired_metric.id]["data_type"]
        == profile.report()["data_stats"][0]["data_type"]
    )
    assert (
        results[desired_metric.id]["categorical"]
        == profile.report()["data_stats"][0]["categorical"]
    )
    assert (
        results[desired_metric.id]["order"]
        == profile.report()["data_stats"][0]["order"]
    )
    assert (
        results[desired_metric.id]["samples"]
        == profile.report()["data_stats"][0]["samples"]
    )
