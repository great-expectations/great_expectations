from abc import ABC, abstractmethod
from typing import List, Union

import numpy as np


class SingleNumericStatisticCalculator(ABC):
    @property
    @abstractmethod
    def sample_identifiers(
        self,
    ) -> List[Union[bytes, str, int, float, complex, tuple, frozenset]]:
        """
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
        Computes numeric statistic from unique identifiers of data samples (a unique identifier must be hashable).
        :parameter: randomized_data_point_identifiers -- List of Hashable objects
        :return: np.float64
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
        Computes numeric statistic from unique identifiers of data samples (a unique identifier must be hashable).
        :parameter: randomized_data_point_identifiers -- List of Hashable objects
        :return: np.float64
        """
        pass
