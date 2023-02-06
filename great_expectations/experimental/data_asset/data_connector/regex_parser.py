from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class RegExParser:
    def __init__(
        self,
        regex_pattern: Optional[re.Pattern] = None,
        unnamed_regex_group_prefix: str = "unnamed_group_",
    ) -> None:
        self._num_all_matched_group_values: int = regex_pattern.groups

        # Check for `(?P<name>)` named group syntax
        self._defined_group_name_to_group_index_mapping: Dict[str, int] = dict(
            regex_pattern.groupindex
        )

        self._regex_pattern: Optional[re.Pattern] = regex_pattern
        self._unnamed_regex_group_prefix: str = unnamed_regex_group_prefix

    def get_num_all_matched_group_values(self) -> int:
        return self._num_all_matched_group_values

    def get_num_defined_matched_group_values(self) -> int:
        return len(self._defined_group_name_to_group_index_mapping)

    def get_defined_group_name_to_group_index_mapping(self) -> Dict[str, int]:
        return self._defined_group_name_to_group_index_mapping

    def get_matches(self, target: str) -> Optional[re.Match]:
        return self._regex_pattern.match(target)

    def get_defined_group_name_to_group_value_mapping(
        self, target: str
    ) -> Dict[str, str]:
        # Check for `(?P<name>)` named group syntax
        return self.get_matches(target=target).groupdict()

    def get_all_matched_group_values(self, target: str) -> List[str]:
        return list(self.get_matches(target=target).groups())

    def get_all_group_names_to_group_indexes_bidirectional_mappings(
        self,
    ) -> Tuple[Dict[str, int], Dict[int, str]]:
        defined_group_index_to_group_name_mapping: Dict[int, str] = dict(
            zip(
                self._defined_group_name_to_group_index_mapping.values(),
                self._defined_group_name_to_group_index_mapping.keys(),
            )
        )

        idx: int
        undefined_group_indexes: List[int] = list(
            filter(
                lambda idx: idx
                not in self._defined_group_name_to_group_index_mapping.values(),
                range(1, self._num_all_matched_group_values + 1),
            )
        )

        group_idx: int
        undefined_group_index_to_group_name_mapping: Dict[int, str] = {
            group_idx: f"{self._unnamed_regex_group_prefix}{group_idx}"
            for group_idx in undefined_group_indexes
        }

        all_group_index_to_group_name_mapping: Dict[int, str] = (
            defined_group_index_to_group_name_mapping
            | undefined_group_index_to_group_name_mapping
        )

        all_group_name_to_group_index_mapping: Dict[str, int] = dict(
            zip(
                all_group_index_to_group_name_mapping.values(),
                all_group_index_to_group_name_mapping.keys(),
            )
        )

        return (
            all_group_name_to_group_index_mapping,
            all_group_index_to_group_name_mapping,
        )

    def get_all_group_name_to_group_index_mapping(self) -> Dict[str, int]:
        all_group_names_to_group_indexes_bidirectional_mappings: Tuple[
            Dict[str, int], Dict[int, str]
        ] = self.get_all_group_names_to_group_indexes_bidirectional_mappings()
        all_group_name_to_group_index_mapping: Dict[
            str, int
        ] = all_group_names_to_group_indexes_bidirectional_mappings[0]
        return all_group_name_to_group_index_mapping

    def get_all_group_index_to_group_name_mapping(self) -> Dict[int, str]:
        all_group_names_to_group_indexes_bidirectional_mappings: Tuple[
            Dict[str, int], Dict[int, str]
        ] = self.get_all_group_names_to_group_indexes_bidirectional_mappings()
        all_group_index_to_group_name_mapping: Dict[
            int, str
        ] = all_group_names_to_group_indexes_bidirectional_mappings[1]
        return all_group_index_to_group_name_mapping

    def get_all_group_names(self) -> List[str]:
        all_group_name_to_group_index_mapping: Dict[
            str, int
        ] = self.get_all_group_name_to_group_index_mapping()
        all_group_names: List[str] = list(all_group_name_to_group_index_mapping.keys())
        return all_group_names

    def get_all_group_indexes(self) -> List[int]:
        all_group_name_to_group_index_mapping: Dict[
            str, int
        ] = self.get_all_group_name_to_group_index_mapping()
        all_group_indexes: List[int] = list(
            all_group_name_to_group_index_mapping.values()
        )
        return all_group_indexes

    def get_group_name_to_group_value_mapping(
        self,
        target: str,
    ) -> Dict[str, str]:
        all_group_names: List[str] = self.get_all_group_names()
        all_matched_group_values: List[str] = self.get_all_matched_group_values(
            target=target
        )
        group_name_to_group_value_mapping: Dict[str, str] = dict(
            zip(all_group_names, all_matched_group_values)
        )
        return group_name_to_group_value_mapping

    def get_group_index_to_group_value_mapping(
        self,
        target: str,
    ) -> Dict[int, str]:
        all_group_indexes: List[int] = self.get_all_group_indexes()
        all_matched_group_values: List[str] = self.get_all_matched_group_values(
            target=target
        )
        group_index_to_group_value_mapping: Dict[int, str] = dict(
            zip(all_group_indexes, all_matched_group_values)
        )
        return group_index_to_group_value_mapping
