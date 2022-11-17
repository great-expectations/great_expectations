from __future__ import annotations

import datetime
import logging
from typing import Any, Dict, Optional

from marshmallow import INCLUDE, Schema, fields, post_load
from ruamel.yaml.comments import CommentedMap

from great_expectations.core.util import ensure_json_serializable
from great_expectations.types.attributes import Attributes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class BatchMetricComputation(Attributes):
    """
    Implements representation for single instance (e.g., as row in database table) of "Batch Metric Computation" record.
    """

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        batch_name: str,
        batch_uuid: str,
        metric_name: str,
        metric_configuration_uuid: str,
        id: Optional[int] = None,
        created_at: Optional[datetime.datetime] = None,
        updated_at: Optional[datetime.datetime] = None,
        deleted_at: Optional[datetime.datetime] = None,
        deleted: bool = False,
        status: int = 0,
        data_context_uuid: Optional[str] = None,
        value: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        fields: dict = {
            "id": id,
            "created_at": created_at,
            "updated_at": updated_at,
            "deleted_at": deleted_at,
            "deleted": deleted,
            "status": status,
            "data_context_uuid": data_context_uuid,
            "datasource_name": datasource_name,
            "data_asset_name": data_asset_name,
            "batch_name": batch_name,
            "batch_uuid": batch_uuid,
            "metric_name": metric_name,
            "metric_configuration_uuid": metric_configuration_uuid,
            "value": value,
            "details": details,
        }
        ensure_json_serializable(data=fields)
        super().__init__(fields)

    def to_dict(self) -> dict:
        this_serialized: CommentedMap = batchMetricComputationSchema.dump(self)
        return dict(this_serialized)


class BatchMetricComputationSchema(Schema):
    class Meta:
        unknown = INCLUDE

    id = fields.Integer(required=False, allow_none=True)
    created_at = fields.DateTime(required=False, allow_none=True)
    updated_at = fields.DateTime(required=False, allow_none=True)
    deleted_at = fields.DateTime(required=False, allow_none=True)
    deleted = fields.Boolean(required=False, allow_none=True, default=False)
    status = fields.Integer(required=False, allow_none=True, default=0)
    data_context_uuid = fields.UUID(required=False, allow_none=True)
    datasource_name = fields.String(required=True, allow_none=False)
    data_asset_name = fields.String(required=True, allow_none=False)
    batch_name = fields.Raw(required=True, allow_none=False)
    batch_uuid = fields.UUID(required=True, allow_none=False)
    metric_name = fields.String(required=True, allow_none=False)
    metric_configuration_uuid = fields.UUID(required=True, allow_none=False)
    value = fields.Raw(required=False, allow_none=True)
    details = fields.Dict(required=False, allow_none=True)

    # noinspection PyUnusedLocal
    @post_load
    def make_batch_metric_computations(self, data: dict, **kwargs):
        return BatchMetricComputation(**data)


batchMetricComputationSchema = BatchMetricComputationSchema()
