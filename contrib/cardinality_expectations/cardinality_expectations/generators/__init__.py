import numpy as np
import pandas as pd

import random
# from dataclasses import dataclass
from pydantic.dataclasses import dataclass
from typing import Optional, Union

from cardinality_expectations.cardinality_categories import (
    FINITE_CATEGORIES,
    INFINITE_CATEGORIES,
)

@dataclass
class CardinalityParams:
    """A dataclass for storing cardinality parameters."""
    is_finite: bool
    category: str

@dataclass
class FiniteCardinalityParams(CardinalityParams):
    """A dataclass for storing finite cardinality parameters."""
    k : int # The exact cardinality
    alpha : float # An exponential skewing parameter. 0 = uniform distribution. Larger a = more skewed. 

@dataclass
class InfiniteCardinalityParams(CardinalityParams):
    """A dataclass for storing infinite cardinality parameters."""
    ratio : float


def gen_cardinality_params() -> CardinalityParams:
    """Generate a random cardinality parameter set."""
    if random.random() > .5:
        is_finite = True
        category = random.choice(FINITE_CATEGORIES)
        
        if category == "A_FEW":
            count = random.choice(range(1,7))
        elif category == "SEVERAL":
            count = random.choice(range(7,20))
        elif category == "MANY":
            count = random.choice(range(20, 200)) #!!! Add a better distribution here
            
        return FiniteCardinalityParams(
            is_finite=is_finite,
            category=category,
            k=count,
            # alpha=random.uniform(0,1)**2,
            alpha=random.uniform(0,.5)**2,
        )
    else:
        is_finite = False
        category = random.choice(INFINITE_CATEGORIES)
        
        if category == "UNIQUE":
            ratio = 1.0
        elif category == "DUPLICATED":
            ratio = random.uniform(0,1)

        return InfiniteCardinalityParams(
            is_finite=is_finite,
            category=category,
            ratio=ratio,
        )

def generate_finite_item(
    k : int,
    alpha : float
) -> int:
    """Generate a random item from a finite set of size k.

    Args:
        k (int): The size of the set.
        alpha (float): An exponential skewing parameter. 0 = uniform distribution. Larger a = more skewed.
    """

    item_indices = np.arange(k)
    probabilities = np.exp(-alpha * item_indices)
    probabilities_sum = np.sum(probabilities)

    r = np.random.rand() * probabilities_sum

    for i, prob in enumerate(probabilities):
        r -= prob
        if r <= 0:
            return i

def gen_data_element(
    cardinality_params: CardinalityParams,
    index: int,
) -> Union[float, int]:
    """Generate a random data element from a given cardinality parameter set.

    Args:
        cardinality_params (CardinalityParams): The cardinality parameter set.
        index (int): The index of the element to generate.
    """
    if cardinality_params.is_finite:
#         value = random.choice(range(cardinality_params.count))
        value = generate_finite_item(
            k = cardinality_params.k,
            alpha = cardinality_params.alpha
        )
        
    else:
        if cardinality_params.ratio == 1.0:
            value = index
            
        else:
            if random.random() < cardinality_params.ratio or index==0 :
                value = index
            else:
                value = random.choice(range(index))
            
    return value
    

def gen_data_series(
    cardinality_params: CardinalityParams,
    n: int,
) -> pd.Series:
    """Generate a random data series from a given cardinality parameter set.

    Args:
        cardinality_params (CardinalityParams): The cardinality parameter set.
        n (int): The number of elements to generate.
    """
    element_list = [gen_data_element(cardinality_params, i) for i in range(n)]
    return pd.Series(element_list)