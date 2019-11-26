from sqlalchemy import create_engine, Column, String, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

from great_expectations.core import ensure_json_serializable, DataAssetIdentifier
from great_expectations.data_context.store.evaluation_parameter_store import EvaluationParameterStore
from great_expectations.data_context.types.metrics import ExpectationDefinedMetricIdentifier
from great_expectations.profile.metrics_utils import kwargs_to_tuple, tuple_to_hash

Base = declarative_base()


class SAExpectationDefinedMetric(Base):
    __tablename__ = 'ge_expectation_defined_metrics'

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

    def get_metric_identifier(self):
        return ExpectationDefinedMetricIdentifier(
            run_id=self.run_id,
            data_asset_name=DataAssetIdentifier(
                datasource=self.datasource,
                generator=self.generator,
                generator_asset=self.generator_asset
            ),
            expectation_suite_name=self.expectation_suite_name,
            expectation_type=self.expectation_type,
            metric_name=self.metric_name,
            metric_kwargs=self.metric_kwargs
        )


class DatabaseEvaluationParameterStore(EvaluationParameterStore):
    # noinspection PyUnusedLocal
    def __init__(self, store_backend, root_directory=None):
        super(DatabaseEvaluationParameterStore, self).__init__(
            store_backend=store_backend,
            root_directory=None,
            serialization_type=None
        )

    def _validate_value(self, value):
        assert ensure_json_serializable(value), "value must be json serializable"

    def _get(self, key):
        metric = self.session.query(
            SAExpectationDefinedMetric
        ).filter_by(
            run_id=key.run_id,
            datasource=key.data_asset_name.datasource,
            generator=key.data_asset_name.generator,
            generator_asset=key.data_asset_name.generator_asset,
            expectation_suite_name=key.expectation_suite_name,
            expectation_type=key.expectation_type,
            metric_name=key.metric_name,
            metric_kwargs_str=tuple_to_hash(
                kwargs_to_tuple(key.metric_kwargs)
            )
        ).first()
        return metric.metric_value

    def _set(self, key, value, **kwargs):
        metric = SAExpectationDefinedMetric(
            run_id=key.run_id,
            datasource=key.data_asset_name.datasource,
            generator=key.data_asset_name.generator,
            generator_asset=key.data_asset_name.generator_asset,
            expectation_suite_name=key.expectation_suite_name,
            expectation_type=key.expectation_type,
            metric_name=key.metric_name,
            metric_kwargs_str=tuple_to_hash(
                kwargs_to_tuple(key.metric_kwargs)
            ),
            metric_kwargs=key.metric_kwargs,
            metric_value=value
        )
        self.session.add(metric)
        self.session.commit()

    def _init_store_backend(self, store_backend_config, runtime_config):
        credentials = store_backend_config.get("credentials", {})
        drivername = credentials.pop("drivername")
        options = URL(drivername, **credentials)
        engine = create_engine(options)
        Base.metadata.create_all(engine)
        self.session = sessionmaker(bind=engine)()

    def get_bind_params(self, run_id):
        metrics = self.session.query(SAExpectationDefinedMetric).filter_by(
            run_id=run_id
        ).all()

        params = {}
        for metric in metrics:
            params[metric.get_metric_identifier().to_urn()] = metric.metric_value
        return params
