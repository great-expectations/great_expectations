import os

import dataprofiler as dp
import pandas as pd

from contrib.capitalone_dataprofiler_expectations.capitalone_dataprofiler_expectations.metrics import (
    DataProfilerColumnProfileReport,
    DataProfilerProfileReport,
)
from great_expectations.self_check.util import build_pandas_engine
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric

_ = DataProfilerColumnProfileReport
_ = DataProfilerProfileReport


test_root_path: str = os.path.dirname(  # noqa: PTH120
    os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # noqa: PTH120
)


def test_data_profiler_column_profile_report_metric_pd():
    engine = build_pandas_engine(
        pd.DataFrame(
            {
                "vendor_id": [
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

    profile_path = os.path.join(  # noqa: PTH118
        test_root_path,
        "data_profiler_files",
        "profile.pkl",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    data_profiler_profile_report_metric: MetricConfiguration = MetricConfiguration(
        metric_name="data_profiler.profile_report",
        metric_domain_kwargs={},
        metric_value_kwargs={
            "profile_path": profile_path,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(data_profiler_profile_report_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="data_profiler.column_profile_report",
        metric_domain_kwargs={"column": "vendor_id"},
        metric_value_kwargs={
            "profile_path": profile_path,
        },
    )
    desired_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
        "data_profiler.profile_report": data_profiler_profile_report_metric,
    }
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
