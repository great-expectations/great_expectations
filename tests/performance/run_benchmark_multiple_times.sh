#!/usr/bin/env bash

# Runs performance tests multiple times.

set -eu

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 [BENCHMARK_JSON_FILE_NAME_PREFIX] [OPTIONAL_PYTEST_ARGS]" >&2
  exit 1
fi

benchmark_json_file_name_prefix=$1

for i in {1..3}
do
  benchmark_json=tests/performance/results/${benchmark_json_file_name_prefix}_run_${i}.json
  date
  set -x
  time pytest tests/performance/test_bigquery_benchmarks.py \
    --benchmark-json=${benchmark_json} \
    --bigquery --performance-tests \
    -q -p no:warnings \
    "${@:2}" \
    2> /dev/null
  set +x
  # Remove some unnecessary personally identifiable fields.
  jq '(del( .machine_info["node", "release"]))' ${benchmark_json} | sponge ${benchmark_json}
done
