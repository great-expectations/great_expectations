from ..dataset import Dataset


class DataSetProfiler(object):

    @classmethod
    def validate_dataset(cls, dataset):
        return issubclass(type(dataset), Dataset)

    @classmethod
    def profile(cls, dataset):
        assert cls.validate_dataset(dataset)
        return cls._profile(dataset)

    @classmethod
    def _profile(cls, dataset):
        raise NotImplementedError
