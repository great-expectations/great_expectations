from abc import ABC, abstractmethod
from typing import List, Union

import numpy as np


class SingleNumericStatisticGetter(ABC):
    @property
    @abstractmethod
    def data_point_identifiers(
        self,
    ) -> List[Union[bytes, str, int, float, complex, tuple, frozenset]]:
        """
        This property must return the list consisting of hashable objects, which identify the data points.

        :return: List of Hashable objects
        """
        pass

    @abstractmethod
    def generate_distribution_sample(
        self,
        randomized_data_point_identifiers: List[
            Union[
                bytes,
                str,
                int,
                float,
                complex,
                tuple,
                frozenset,
            ]
        ],
    ) -> Union[
        np.ndarray, List[Union[int, np.int32, np.int64, float, np.float32, np.float64]]
    ]:
        """
        Given a randomized list of data point identifiers, it computes metrics corresponding to each data point and
        returns the collection of these metrics as a sample of the distribution, where the dimensionality of
        the sample of the distribution is equal to the number of the identifiers provided (variable length is accepted).

        :parameter: randomized_data_point_identifiers -- List of Hashable objects
        :return: An array-like (or list-like) collection of floating point numbers, representing the distribution sample
        """
        pass

    @abstractmethod
    def compute_numeric_statistic(
        self,
        randomized_data_point_identifiers: List[
            Union[
                bytes,
                str,
                int,
                float,
                complex,
                tuple,
                frozenset,
            ]
        ],
    ) -> np.float64:
        """
        Given a randomized list of data point identifiers, it samples the distribution and computes a statistic for that
        sample.  Any single-valued numeric statistic that is a function of the data points is acceptable.

        :parameter: randomized_data_point_identifiers -- List of Hashable objects
        :return: np.float64 -- scalar measure of the distribution sample, characterized by the data point identifiers
        """
        pass
