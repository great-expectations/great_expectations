import json
import time

from ..dataset import Dataset


class DataSetProfiler(object):

    @classmethod
    def validate_dataset(cls, dataset):
        return issubclass(type(dataset), Dataset)

    @classmethod
    def add_meta(cls, expectations_config):
        if not "meta" in expectations_config:
            expectations_config["meta"] = {}

        class_name = str(cls.__name__)
        expectations_config["meta"][class_name] = {
            "created_by": class_name,
            "created_at": time.time(),
            # "batch_kwargs": {},
        }

        print(expectations_config["meta"])
        return expectations_config

    @classmethod
    def profile(cls, dataset):
        assert cls.validate_dataset(dataset)
        expectations_config = cls._profile(dataset)
        expectations_config = cls.add_meta(expectations_config)
        validation_results = None  # dataset.validate(expectations_config)
        return expectations_config, validation_results

    @classmethod
    def _profile(cls, dataset):
        raise NotImplementedError
