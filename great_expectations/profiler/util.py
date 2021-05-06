from typing import List

from great_expectations import DataContext
from great_expectations.core.batch import BatchDefinition, BatchRequest


def get_batch_ids(data_context: DataContext, batch_request: BatchRequest) -> List[str]:
    datasource_name: str = batch_request.datasource_name
    batch_definitions: List[BatchDefinition] = data_context.get_datasource(
        datasource_name=datasource_name
    ).get_batch_definition_list_from_batch_request(batch_request=batch_request)
    return [batch_definition.id for batch_definition in batch_definitions]
