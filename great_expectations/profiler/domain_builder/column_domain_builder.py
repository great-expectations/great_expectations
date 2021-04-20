from great_expectations.execution_engine.execution_engine import MetricDomainTypes

from ..exceptions import ProfilerConfigurationError
from .domain_builder import DomainBuilder


# TODO: <Alex>ALEX -- How come this class overwrites get_domains() but not _get_domains() -- was this intentional?</Alex>
class ColumnDomainBuilder(DomainBuilder):
    def get_domains(
        self,
        *,
        validator=None,
        batch_ids=None,
        domain_type: MetricDomainTypes = None,
        **kwargs,
    ):
        """
        Obtains and returns a given column
        """
        if domain_type is not None and domain_type != MetricDomainTypes.COLUMN:
            raise ProfilerConfigurationError(
                f"{self.__class__.__name__} requires a COLUMN domain."
            )

        # TODO: <Alex>ALEX -- perhaps an opportunity to clean this up?</Alex>
        # Todo: This is calling an unimplemented parent implementation, not currently functional <Gil>
        return super().get_domains(
            validator=validator, batch_ids=batch_ids, domain_type=domain_type, **kwargs
        )

    # TODO: <Alex>ALEX -- this method is defined, but not used anywhere in the codebase.</Alex>
    def get_column_domains(self, *, validator=None, batch_ids=None, **kwargs):
        """
        Pops column domain out of a dict of certain domain kwargs and requests this domain
        """
        domain_type = kwargs.pop("domain_type", MetricDomainTypes.COLUMN)
        return self.get_domains(
            validator=validator, batch_ids=batch_ids, domain_type=domain_type, **kwargs
        )
