from dataclasses import dataclass
from typing import Any, Dict, List

class TestData(dict):
    pass

@dataclass
class ExpectationTestCase:
    title: str
    input: Dict[str, Any]
    output: Dict[str, Any]
    exact_match_out: bool
    suppress_test_for: List[str]

class ExpectationLegacyTestCaseAdapter(ExpectationTestCase):
    """This class provides an adapter between the test cases developed prior to Great Expectations' 0.14 release and the newer ExpectationTestCase dataclass
    
    Notes:
    * Legacy test cases used "in" (a python reserved word). This has been changed to "input".
    * To maintain parallelism, we've also made the corresponding change from "out" to "output".
    * To avoid any ambiguity, ExpectationLegacyTestCaseAdapter only accepts keyword arguments. Positional arguments are not allowed.
    """

    def __init__(self,
        *,
        title,
        exact_match_out,
        suppress_test_for,
        out,
        **kwargs,
    ):
        super().__init__(
            title=title,
            input=kwargs["in"],
            output=out,
            exact_match_out=exact_match_out,
            suppress_test_for=suppress_test_for,
        )

@dataclass
class ExpectationTestDataCases:
    data : TestData
    tests : List[ExpectationTestCase]