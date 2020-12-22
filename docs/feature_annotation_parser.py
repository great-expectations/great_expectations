import ast
import json
import logging
import os
import re
from collections import namedtuple
from typing import Dict, Iterator, List, Mapping, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)

ANNOTATION_REGEX = r""
ANNOTATION_REGEX += r"[\s]*(id:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(title:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(icon:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(short_description:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(description:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(how_to_guide_url:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(maturity:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(maturity_details:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(api_stability:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(implementation_completeness:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(unit_test_coverage:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(integration_infrastructure_test_coverage:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(documentation_completeness:.*)[\n]"
ANNOTATION_REGEX += r"[\s]*(bug_risk:.*)[\n]"
ANNOTATION_REGEX += r"([\s]*(expectation_completeness:.*)[\n])?"
annotation_regex_compiled = re.compile(ANNOTATION_REGEX)


maturity_details_keys = [
    "api_stability",
    "implementation_completeness",
    "unit_test_coverage",
    "integration_infrastructure_test_coverage",
    "documentation_completeness",
    "bug_risk",
    "expectation_completeness",
]

AnnotatedNode = namedtuple("AnnotatedNode", ["name", "path", "annotation", "type_"])


def parse_feature_annotation(docstring: Union[str, List[str], None]):
    """Parse a docstring and return a feature annotation."""
    list_of_annotations = []
    id_val = ""
    if docstring is None:
        return
    if isinstance(docstring, str):
        for matches in re.findall(annotation_regex_compiled, docstring):
            annotation_dict = dict()  # create new dictionary for each match
            maturity_details_dict = dict()
            for matched_line in matches:
                if not matched_line:
                    continue
                # split matched line_fields
                matched_line_fields = matched_line.split(":")
                this_key = matched_line_fields[0].strip()
                this_val = (
                    ""
                    if "TODO" in (":".join(matched_line_fields[1:]).strip())
                    else (":".join(matched_line_fields[1:]).strip())
                )
                if this_key == "id":
                    id_val = this_val

                if this_key in maturity_details_keys:
                    maturity_details_dict[this_key] = this_val
                elif this_key == "icon":  # icon is a special cases
                    if this_val == "":
                        annotation_dict[
                            this_key
                        ] = f"https://great-expectations-web-assets.s3.us-east-2.amazonaws.com/feature_maturity_icons/{id_val}.png"
                    else:
                        annotation_dict[this_key] = this_val
                else:
                    annotation_dict[this_key] = this_val

            annotation_dict["maturity_details"] = maturity_details_dict
            if annotation_dict is not None:
                list_of_annotations.append(annotation_dict)
    return list_of_annotations
