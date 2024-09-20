from __future__ import annotations

import logging
import re
from typing import Dict, List, Match, Optional, Tuple

logger = logging.getLogger(__name__)


class RegExParser:
    def __init__(
        self,
        regex_pattern: re.Pattern,
        unnamed_regex_group_prefix: str = "unnamed_group_",
    ) -> None:
        self._num_all_matched_group_values: int = regex_pattern.groups

        # Check for `(?P<name>)` named group syntax
        self._group_name_to_index_dict: Dict[str, int] = dict(regex_pattern.groupindex)

        self._regex_pattern: re.Pattern = regex_pattern
        self._unnamed_regex_group_prefix: str = unnamed_regex_group_prefix

    def get_num_all_matched_group_values(self) -> int:
        return self._num_all_matched_group_values

    def get_named_group_name_to_group_index_mapping(self) -> Dict[str, int]:
        return self._group_name_to_index_dict

    def get_matches(self, target: str) -> Optional[Match[str]]:
        return self._regex_pattern.match(target)

    def get_all_group_names_to_group_indexes_bidirectional_mappings(
        self,
    ) -> Tuple[Dict[str, int], Dict[int, str]]:
        named_group_index_to_group_name_mapping: Dict[int, str] = dict(
            zip(
                self._group_name_to_index_dict.values(),
                self._group_name_to_index_dict.keys(),
            )
        )

        idx: int
        common_group_indexes: List[int] = list(
            filter(
                lambda idx: idx not in self._group_name_to_index_dict.values(),
                range(1, self._num_all_matched_group_values + 1),
            )
        )

        group_idx: int
        common_group_index_to_group_name_mapping: Dict[int, str] = {
            group_idx: f"{self._unnamed_regex_group_prefix}{group_idx}"
            for group_idx in common_group_indexes
        }

        all_group_index_to_group_name_mapping: Dict[int, str] = {
            **named_group_index_to_group_name_mapping,
            **common_group_index_to_group_name_mapping,
        }

        element: Tuple[int, str]
        # noinspection PyTypeChecker
        all_group_index_to_group_name_mapping = dict(
            sorted(
                all_group_index_to_group_name_mapping.items(),
                key=lambda element: element[0],
                reverse=False,
            )
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
        all_group_name_to_group_index_mapping: Dict[str, int] = (
            all_group_names_to_group_indexes_bidirectional_mappings[0]
        )
        return all_group_name_to_group_index_mapping

    def get_all_group_index_to_group_name_mapping(self) -> Dict[int, str]:
        all_group_names_to_group_indexes_bidirectional_mappings: Tuple[
            Dict[str, int], Dict[int, str]
        ] = self.get_all_group_names_to_group_indexes_bidirectional_mappings()
        all_group_index_to_group_name_mapping: Dict[int, str] = (
            all_group_names_to_group_indexes_bidirectional_mappings[1]
        )
        return all_group_index_to_group_name_mapping

    def group_names(self) -> List[str]:
        all_group_name_to_group_index_mapping: Dict[str, int] = (
            self.get_all_group_name_to_group_index_mapping()
        )
        all_group_names: List[str] = list(all_group_name_to_group_index_mapping.keys())
        return all_group_names
