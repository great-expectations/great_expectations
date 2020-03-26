import pytest
import jsonschema

from great_expectations.core.logging.schemas import (
    anonymized_name_schema,
    anonymized_datasource_schema,
    init_payload_schema,
    usage_statistics_record_schema,
)


def test_anonymized_name_validation():
    string = "aa41efe0a1b3eeb9bf303e4561ff8392"
    jsonschema.validate(string, anonymized_name_schema)

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(string[:5], anonymized_name_schema)


def test_anonymized_datasource_validation():
    record = {
        "anonymized_name": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "parent_class": "hello"
    }
    jsonschema.validate(record, anonymized_datasource_schema)

    record = {
        "anonymized_name": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "parent_class": "hello",
        "anonymized_class": "aa41efe0a1b3eeb9bf303e4561ff8392",
    }
    jsonschema.validate(record, anonymized_datasource_schema)
    record = {
        "anonymized_name": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "parent_class": "SparkDFDataset",
        "anonymized_class": "aa41efe0a1b3eeb9bf303e4561ff8392",
        "engine": "postgres"
    }
    jsonschema.validate(record, anonymized_datasource_schema)


def test_init_payload_validation():
    payload = {
        "platform.system": "Darwin",
        "platform.release": "19.4.0",
        "version_info": "sys.version_info(major=3, minor=7, micro=5, releaselevel='final', serial=0)",
        "anonymized_datasources": [
            {
                "parent_class": "PandasDatasource"
            }
        ]
    }
    jsonschema.validate(payload, init_payload_schema)


def test_payload_validation():
    payload = {
        "platform.system": "Darwin",
        "platform.release": "19.4.0",
        "version_info": "sys.version_info(major=3, minor=7, micro=5, releaselevel='final', serial=0)",
        "anonymized_datasources": [
            {
                "parent_class": "PandasDatasource"
            }
        ]
    }
    message = {
        "version": "1.0.0",
        "event": "data_context.__init_",
        "success": True,
        "event_time": "2020-03-26T11:17:50.460Z",
        "data_context_id": "705dd2a2-27f8-470f-9ebe-e7058fd7a534",
        "data_context_instance_id": "ff72f73e-bf2e-48d6-afb8-b646d14e83b8",
        "ge_version": "0.10.0",
        "event_payload": payload
    }
    jsonschema.validate(message, usage_statistics_record_schema)
