from typing import Optional, List, Dict, Any, Union

from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.validator.validator import Validator
from great_expectations.profiler.domain_builder.domain_builder import DomainBuilder
import great_expectations.exceptions as ge_exceptions


# TODO: <Alex>ALEX -- The fact that this class overwrites get_domains() instead of _get_domains() is problematic and should be changed to the standarized approach (the latter).</Alex>
class ColumnDomainBuilder(DomainBuilder):
    # TODO: <Alex>ALEX -- What is the return type?</Alex>
    def get_domains(
        self,
        *,
        validator: Optional[Validator] = None,
        batch_ids: Optional[List[str]] = None,
        include_batch_id: Optional[bool] = False,
        domain_type: Optional[MetricDomainTypes] = None,
        **kwargs
    ) -> List[Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]]:
        """
        Obtains and returns a given column
        """
        if domain_type is not None and domain_type != MetricDomainTypes.COLUMN:
            raise ge_exceptions.ProfilerConfigurationError(
                message=f"{self.__class__.__name__} requires a COLUMN domain."
            )

        # TODO: <Alex>ALEX -- perhaps an opportunity to clean this up?</Alex>
        # Todo: This is calling an unimplemented parent implementation, not currently functional <Gil>
        return super().get_domains(
            validator=validator, batch_ids=batch_ids, domain_type=domain_type, **kwargs
        )

    # TODO: <Alex>ALEX -- this method is defined, but not used anywhere in the codebase.</Alex>
    # TODO: <Alex>ALEX -- What is the return type?</Alex>
    def get_column_domains(self, *, validator=None, batch_ids=None, **kwargs) -> List[Dict[str, Union[str, MetricDomainTypes, Dict[str, Any]]]]:
        """
        Pops column domain out of a dict of certain domain kwargs and requests this domain
        """
        domain_type = kwargs.pop("domain_type", MetricDomainTypes.COLUMN)
        return self.get_domains(
            validator=validator, batch_ids=batch_ids, domain_type=domain_type, **kwargs
        )
