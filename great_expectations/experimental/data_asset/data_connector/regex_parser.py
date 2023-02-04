import logging
import re
from typing import Dict, List, Optional

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

    def get_all_group_names(
        self,
    ) -> List[str]:
        num_all_matched_group_values: int = self.get_num_all_matched_group_values()
        num_defined_matched_group_values: int = (
            self.get_num_defined_matched_group_values()
        )
        defined_group_name_to_group_index_mapping: Dict[
            str, int
        ] = self.get_defined_group_name_to_group_index_mapping()
        defined_group_index_to_group_name_mapping: Dict[int, str] = dict(
            zip(
                defined_group_name_to_group_index_mapping.values(),
                defined_group_name_to_group_index_mapping.keys(),
            )
        )

        all_group_names: List[str] = list(
            defined_group_index_to_group_name_mapping.values()
        )

        idx: int
        group_idx: int
        group_name: str
        for idx in range(
            num_defined_matched_group_values, num_all_matched_group_values
        ):
            group_idx = idx + 1
            group_name = f"{self._unnamed_regex_group_prefix}{group_idx}"
            all_group_names.append(group_name)

        return all_group_names

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
