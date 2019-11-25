from marshmallow import Schema, fields, post_load

from sqlalchemy import create_engine, Column, String, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

from great_expectations.core import ensure_json_serializable, DataAssetIdentifierSchema, DataAssetIdentifier
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.metrics import ExpectationDefinedMetricIdentifier, \
    ExpectationDefinedValidationMetric
from great_expectations.profile.metrics_utils import kwargs_to_tuple, tuple_to_hash

Base = declarative_base()


class SAExpectationDefinedValidationMetric(Base):
    __tablename__ = 'ge_expectation_defined_validation_metrics'

    run_id = Column(String, primary_key=True)
    datasource = Column(String, primary_key=True)
    generator = Column(String, primary_key=True)
    generator_asset = Column(String, primary_key=True)
    expectation_suite_name = Column(String, primary_key=True)
    expectation_type = Column(String, primary_key=True)
    metric_name = Column(String, primary_key=True)
    metric_kwargs_str = Column(String, primary_key=True)
    metric_kwargs = Column(JSON)
    metric_value = Column(JSON)

    def make_expectation_defined_validation_metric(self):
        return ExpectationDefinedValidationMetric(
            run_id=self.run_id,
            metric_identifier=ExpectationDefinedMetricIdentifier(
                data_asset_name=DataAssetIdentifier(
                    datasource=self.datasource,
                    generator=self.generator,
                    generator_asset=self.generator_asset
                ),
                expectation_suite_name=self.expectation_suite_name,
                expectation_type=self.expectation_type,
                metric_name=self.metric_name,
                metric_kwargs=self.metric_kwargs
            ),
            metric_value=self.metric_value
        )


class ExpectationDefinedMetricIdentifierSchema(Schema):
    data_asset_name = fields.Nested(DataAssetIdentifierSchema)
    expectation_suite_name = fields.Str()
    expectation_type = fields.Str()
    metric_kwargs = fields.Dict()
    metric_name = fields.Str()

    @post_load
    def make_expectation_defined_metric_identifier(self, data):
        return ExpectationDefinedMetricIdentifier(**data)


class ValidationMetricSchema(Schema):
    run_id = fields.Str()
    expectation_defined_metric_identifier = fields.Nested(ExpectationDefinedMetricIdentifierSchema)


class DatabaseStoreBackend(StoreBackend):
    def list_keys(self):
        pass

    def _has_key(self, key):
        pass

    def __init__(self, credentials):
        drivername = credentials.pop("drivername")
        options = URL(drivername, **credentials)
        engine = create_engine(options)
        Base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()
        super(DatabaseStoreBackend, self).__init__()

    def _validate_value(self, value):
        assert ensure_json_serializable(value), "value must be json serializable"

    def _get(self, key):
        metric = self.session.query(
            SAExpectationDefinedValidationMetric
        ).filter_by(
            run_id=key.run_id,
            datasource=key.metric_identifier.data_asset_name.datasource,
            generator=key.metric_identifier.data_asset_name.generator,
            generator_asset=key.metric_identifier.data_asset_name.generator_asset,
            expectation_suite_name=key.metric_identifier.expectation_suite_name,
            expectation_type=key.metric_identifier.expectation_type,
            metric_name=key.metric_identifier.metric_name,
            metric_kwargs_str=tuple_to_hash(
                kwargs_to_tuple(key.metric_identifier.metric_kwargs)
            )
        ).first()
        return metric.make_expectation_defined_validation_metric()

    def _set(self, key, value, **kwargs):
        SAExpectationDefinedValidationMetric(
            run_id=key.run_id,
            datasource=key.metric_identifier.data_asset_name.datasource,
            generator=key.metric_identifier.data_asset_name.generator,
            generator_asset=key.metric_identifier.data_asset_name.generator_asset,
            expectation_suite_name=key.metric_identifier.expectation_suite_name,
            expectation_type=key.metric_identifier.expectation_type,
            metric_name=key.metric_identifier.metric_name,
            metric_kwargs_str=tuple_to_hash(
                kwargs_to_tuple(key.metric_identifier.metric_kwargs)
            ),
            metric_kwargs=key.metric_identifier.metric_kwargs,
            metric_value=value
        )
