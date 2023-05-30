import pytest
import random
import numpy as np

from cardinality_expectations.generators import (
    FiniteCardinalityParams,
    gen_cardinality_params,
    gen_data_element,
    gen_data_series,
)

@pytest.fixture
def example_cardinality_params():
    return FiniteCardinalityParams(
        is_finite=True,
        category='SEVERAL',
        k=7,
        alpha=0.25891675029296335
    )

def test__imports():
    pass


def test__generate_cardinality_params():
    np.random.seed(0)
    random.seed(0)

    params = gen_cardinality_params()
    assert params == FiniteCardinalityParams(
        is_finite=True,
        category='SEVERAL',
        k=7,
        alpha=0.25891675029296335
    )

def test__generate_data_element(example_cardinality_params):
    np.random.seed(0)
    random.seed(0)

    element = gen_data_element(
        cardinality_params=example_cardinality_params,
        index=0
    )
    assert element == 2

def test__generate_data_series(example_cardinality_params):
    np.random.seed(0)
    random.seed(0)

    series = gen_data_series(
        cardinality_params=example_cardinality_params,
        n=10
    )
    assert list(series) == [2,3,2,2,1,3,1,5,6,1]