from great_expectations.experimental.metric_repository.metrics import MetricRun
from great_expectations.experimental.metric_repository.serializer import Serializer


class MetricRunSerializer(Serializer[MetricRun]):
    def serialize(self, value: MetricRun) -> str:
        return value.json()
