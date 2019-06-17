import json
import time

from ..data_asset import DataAsset
from ..dataset import Dataset


class DataAssetProfiler(object):

    @classmethod
    def validate(cls, data_asset):
        return isinstance(data_asset, DataAsset)

class DatasetProfiler(object):

    @classmethod
    def validate(cls, dataset):
        return isinstance(dataset, Dataset)

    @classmethod
    def add_expectation_meta(cls, expectation):
        if not "meta" in expectation:
            expectation["meta"] = {}

        expectation["meta"][str(cls.__name__)] = {
            "confidence": "very low"
        }
        return expectation

    @classmethod
    def add_meta(cls, expectations_config, batch_kwargs=None):
        if not "meta" in expectations_config:
            expectations_config["meta"] = {}

        class_name = str(cls.__name__)
        expectations_config["meta"][class_name] = {
            "created_by": class_name,
            "created_at": time.time(),
        }

        if batch_kwargs != None:
            expectations_config["meta"][class_name]["batch_kwargs"] = batch_kwargs

        new_expectations = [cls.add_expectation_meta(
            exp) for exp in expectations_config["expectations"]]
        expectations_config["expectations"] = new_expectations

        return expectations_config

    @classmethod
    def profile(cls, dataset):
        # TODO: Consider raising a more descriptive error here
        assert cls.validate(dataset)
        expectations_config = cls._profile(dataset)

        batch_kwargs = dataset.get_batch_kwargs()
        expectations_config = cls.add_meta(expectations_config, batch_kwargs)
        validation_results = dataset.validate(expectations_config)
        return expectations_config, validation_results

    @classmethod
    def _profile(cls, dataset):
        raise NotImplementedError
