import yaml
import json
import random
import datetime
import sqlite3

import pandas as pd

from great_expectations.execution_environment.data_connector import (
    SqlDataConnector,
)
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionDefinition,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config
)
from tests.test_utils import (
    create_fake_data_frame,
    create_files_in_directory,
)

from great_expectations.data_context.util import instantiate_class_from_config

def generate_ascending_list_of_datetimes(
    k,
    start_date=datetime.date(2020,1,1),
    end_date=datetime.date(2020,12,31)
):
    start_time = datetime.datetime(start_date.year, start_date.month, start_date.day)
    days_between_dates = (end_date - start_date).total_seconds()
    
    datetime_list = [start_time + datetime.timedelta(seconds=random.randrange(days_between_dates)) for i in range(k)]
    datetime_list.sort()
    return datetime_list

def gen_db(base_directory):
    k = 120
    random.seed(1)

    timestamp_list = generate_ascending_list_of_datetimes(k, end_date=datetime.date(2020,1,31))
    date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

    batch_ids = [random.randint(0,10) for i in range(k)]
    batch_ids.sort()

    session_ids = [random.randint(2,60) for i in range(k)]
    session_ids.sort()
    session_ids = [i-random.randint(0,2) for i in session_ids]

    events_df = pd.DataFrame({
        "id" : range(k),
        "batch_id" : batch_ids,
        "date" : date_list,
        "y" : [d.year for d in date_list],
        "m" : [d.month for d in date_list],
        "d" : [d.day for d in date_list],
        "timestamp" : timestamp_list,
        "session_id" : session_ids,
        "event_type" : [random.choice(["start", "stop", "continue"]) for i in range(k)],
        "favorite_color" : ["#"+"".join([random.choice(list("0123456789ABCDEF")) for j in range(6)]) for i in range(k)]
    })

    db = sqlite3.connect(base_directory+"/temp_db.db")
    events_df.to_sql("events_df", db)

    return db



def test_basic_self_check(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_basic_self_check"))
    db = gen_db(base_directory)

    config = yaml.load("""
    name: my_sql_data_connector
    execution_environment_name: FAKE_EE_NAME

    assets:
        events_df:
            #table_name: events # If table_name is omitted, then the table_name defaults to the asset name
            splitter:
                column_name: date
    """, yaml.FullLoader)
    config["execution_engine"] = db

    my_data_connector = SqlDataConnector(**config)

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "SqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "events_df"
        ],
        "data_assets": {
            "events_df": {
                "batch_definition_count": 30,
                "example_data_references": [
                    "2020-01-01",
                    "2020-01-02",
                    "2020-01-03"
                ]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }