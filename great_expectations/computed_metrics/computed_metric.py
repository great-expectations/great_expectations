from __future__ import annotations

import datetime
import logging
from typing import Any, Dict, Optional

from marshmallow import INCLUDE, Schema, fields, post_load
from ruamel.yaml.comments import CommentedMap

from great_expectations.types.attributes import Attributes

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ComputedMetric(Attributes):
    """
    Implements representation for single instance (e.g., as row in database table) of "ComputedMetric" record.
    """

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        batch_id: str,
        metric_name: str,
        metric_domain_kwargs_id: str,
        metric_value_kwargs_id: str,
        datasource_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        batch_name: Optional[str] = None,
        id: Optional[int] = None,
        created_at: Optional[datetime.datetime] = None,
        updated_at: Optional[datetime.datetime] = None,
        deleted_at: Optional[datetime.datetime] = None,
        deleted: bool = False,
        archived_at: Optional[datetime.datetime] = None,
        archived: bool = False,
        # TODO: <Alex>ALEX</Alex>
        # status: int = 0,
        # TODO: <Alex>ALEX</Alex>
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
            "archived_at": archived_at,
            "archived": archived,
            # TODO: <Alex>ALEX</Alex>
            # "status": status,
            # TODO: <Alex>ALEX</Alex>
            "data_context_uuid": data_context_uuid,
            "datasource_name": datasource_name,
            "data_asset_name": data_asset_name,
            "batch_name": batch_name,
            "batch_id": batch_id,
            "metric_name": metric_name,
            "metric_domain_kwargs_id": metric_domain_kwargs_id,
            "metric_value_kwargs_id": metric_value_kwargs_id,
            "value": value,
            "details": details,
        }
        super().__init__(fields)

    def to_dict(self) -> dict:
        this_serialized: CommentedMap = computedMetricSchema.dump(self)
        return dict(this_serialized)


class ComputedMetricSchema(Schema):
    class Meta:
        unknown = INCLUDE

    id = fields.Integer(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    # created_at = fields.DateTime(required=False, allow_none=True)
    # updated_at = fields.DateTime(required=False, allow_none=True)
    # deleted_at = fields.DateTime(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    created_at = fields.Raw(required=False, allow_none=True)
    updated_at = fields.Raw(required=False, allow_none=True)
    deleted_at = fields.Raw(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    deleted = fields.Boolean(required=False, allow_none=True, default=False)
    # TODO: <Alex>ALEX</Alex>
    # archived_at = fields.DateTime(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    archived_at = fields.Raw(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    archived = fields.Boolean(required=False, allow_none=True, default=False)
    # TODO: <Alex>ALEX</Alex>
    # status = fields.Integer(required=False, allow_none=True, default=0)
    # TODO: <Alex>ALEX</Alex>
    data_context_uuid = fields.UUID(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    # datasource_name = fields.String(required=True, allow_none=True)
    # data_asset_name = fields.String(required=True, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    datasource_name = fields.String(required=False, allow_none=True)
    data_asset_name = fields.String(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    # batch_name = fields.Raw(required=True, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    # TODO: <Alex>ALEX</Alex>
    batch_name = fields.Raw(required=False, allow_none=True)
    # TODO: <Alex>ALEX</Alex>
    batch_id = fields.String(required=True, allow_none=False)
    metric_name = fields.String(required=True, allow_none=False)
    metric_domain_kwargs_id = fields.String(required=True, allow_none=False)
    metric_value_kwargs_id = fields.String(required=True, allow_none=True)
    value = fields.Raw(required=False, allow_none=True)
    details = fields.Dict(required=False, allow_none=True)

    # noinspection PyUnusedLocal
    @post_load
    def make_metric_computation(self, data: dict, **kwargs):
        return ComputedMetric(**data)


computedMetricSchema = ComputedMetricSchema()
