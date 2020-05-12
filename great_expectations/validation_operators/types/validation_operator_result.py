class ValidationOperatorResult(object):
    # TODO this is a design sketch
    def __init__(self):
        raise NotImplementedError

    def get_run_id(self):
        raise NotImplementedError

    def list_assets_validated(self) -> list:
        raise NotImplementedError

    def get_evaluation_parameters(self):
        raise NotImplementedError

    @property
    def statistics(self) -> dict:
        raise NotImplementedError

    @property
    def actions_results(self) -> dict:
        raise NotImplementedError

    @property
    def success(self) -> bool:
        raise NotImplementedError

    @property
    def details(self):
        # TODO I'm not sure what type this should return
        raise NotImplementedError

    @property
    def validation_result_by_expectation_suite_identifier(self) -> dict:
        raise NotImplementedError