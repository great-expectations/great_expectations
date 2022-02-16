from datetime import datetime
from timeit import default_timer as timer

import numpy as np
import pandas as pd

from great_expectations.dataset.pandas_dataset import PandasDataset

np.random.seed(42)

N_ROWS = 1_000_000
COL_NAME = "x"
N = 10

INT_MIN = 0.0
INT_MAX = 10_000

FLOAT_MIN = 0.0
FLOAT_MAX = 10_000.0

DT_MIN = datetime(2020, 1, 1)
DT_MAX = datetime(2021, 1, 1)


dataset_int = PandasDataset(
    {COL_NAME: pd.Series(np.random.randint(low=INT_MIN, high=INT_MAX, size=N_ROWS))}
)
dataset_float = PandasDataset(
    {COL_NAME: pd.Series(np.random.uniform(low=FLOAT_MIN, high=FLOAT_MAX, size=N_ROWS))}
)

dataset_dt = PandasDataset(
    {
        COL_NAME: tuple(
            datetime.fromtimestamp(val).isoformat()
            for val in np.random.randint(
                low=DT_MIN.timestamp(), high=DT_MAX.timestamp(), size=N_ROWS // 100
            )
        )
    }
)


benchmark_int = lambda: dataset_int.expect_column_values_to_be_between(
    column=COL_NAME, min_value=INT_MIN, max_value=INT_MAX
)
benchmark_float = lambda: dataset_float.expect_column_values_to_be_between(
    column=COL_NAME, min_value=FLOAT_MIN, max_value=FLOAT_MAX
)
benchmark_dt = lambda: dataset_dt.expect_column_values_to_be_between(
    column=COL_NAME,
    min_value=DT_MIN.isoformat(),
    max_value=DT_MAX.isoformat(),
    parse_strings_as_datetimes=True,
)


def _time(func):
    start = timer()
    func()
    return timer() - start


def time_multiple(func, n):
    return tuple(_time(func) for _ in range(n))


if __name__ == "__main__":
    measurements_int = time_multiple(benchmark_int, N)
    measurements_float = time_multiple(benchmark_float, N)
    measurements_dt = time_multiple(benchmark_dt, N)
    results = pd.DataFrame(
        {
            "int": measurements_int,
            "float": measurements_float,
            "datetime": measurements_dt,
        }
    )
    print(results)
    results.to_csv("benchmark.csv", index=False)
