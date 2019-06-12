from ..dataset import Dataset


class DataSetProfiler(object):

    @classmethod
    def validate_dataset(cls, dataset):
        return issubclass(type(dataset), Dataset)

    @classmethod
    def profile(cls, dataset):
        assert cls.validate_dataset(dataset)
        expectations_config = cls._profile(dataset)
        validation_results = None  # dataset.validate(expectations_config)
        return expectations_config, validation_results

    @classmethod
    def _profile(cls, dataset):
        raise NotImplementedError
