from great_expectations.exceptions import GreatExpectationsError


class UnsupportedExpectationConfiguration(GreatExpectationsError):
    pass


class UnsupportedDLTExpectationConfiguration(GreatExpectationsError):
    pass
