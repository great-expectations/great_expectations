# Configuring Data Before Running Performance Tests

These performance tests use BigQuery.

Before running a performance test, setup data with `setup_bigquery_tables_for_performance_test.sh`.

For example:

```bash
GE_TEST_BIGQUERY_PEFORMANCE_DATASET=<YOUR_GCP_PROJECT> setup_bigquery_tables_for_performance_test.sh
```

For more information on getting started with BigQuery, please refer to the [contributing BigQuery tests documentation](https://docs.greatexpectations.io/docs/contributing/contributing_test#bigquery-tests).

# Running the Performance Tests

Run the performance tests with pytest, e.g.

```
pytest test_bigquery_benchmarks.py \
  --bigquery --performance-tests \
  -k test_bikeshare_trips_benchmark[1] \
  --benchmark-json=tests/performance/results/`date "+%H%M"`_${USER}.json \
  --no-spark --no-postgresql -rP -vv
```

Some benchmarks take a long time to complete. In this example, only the relatively fast `test_bikeshare_trips_benchmark[1]` benchmark is run and the output should include runtime like the following:

```
--------------------------------------------------- benchmark: 1 tests ---------------------------------------------------
Name (time in s)                         Min     Max    Mean  StdDev  Median     IQR  Outliers     OPS  Rounds  Iterations
--------------------------------------------------------------------------------------------------------------------------
test_bikeshare_trips_benchmark[1]     8.2794  8.2794  8.2794  0.0000  8.2794  0.0000       0;0  0.1208       1           1
--------------------------------------------------------------------------------------------------------------------------
```

The result is saved for comparisons as described below.

# Comparing Performance Results

Compare test results in this directory with `py.test-benchmark compare`, e.g.

```
$ py.test-benchmark compare --group-by name results/initial_baseline.json results/*${USER}.json

---------------------------------------------------------------------------- benchmark 'test_bikeshare_trips_benchmark[1]': 2 tests ---------------------------------------------------------------------------
Name (time in s)                                        Min               Max              Mean            StdDev            Median               IQR            Outliers     OPS            Rounds  Iterations
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_bikeshare_trips_benchmark[1] (initial_base)     6.4498 (1.0)      6.4498 (1.0)      6.4498 (1.0)      0.0000 (1.0)      6.4498 (1.0)      0.0000 (1.0)           0;0  0.1550 (1.0)           1           1
test_bikeshare_trips_benchmark[1] (1623_jdimatt)     7.4123 (1.15)     7.4123 (1.15)     7.4123 (1.15)     0.0000 (1.0)      7.4123 (1.15)     0.0000 (1.0)           0;0  0.1349 (0.87)          1           1
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

Please refer to [pytest-benchmark documentation](https://pytest-benchmark.readthedocs.io/en/latest/comparing.html) for more info.
