from typing import List

import pytest


@pytest.fixture
def bob_columnar_table_multi_batch():
    """
    About the "Bob" User Workflow Fixture

    Bob is working with monthly batches of taxi fare data. His data has the following columns:vendor_id,pickup_datetime,dropoff_datetime,passenger_count,trip_distance,rate_code_id,store_and_fwd_flag,pickup_location_id,dropoff_location_id,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge

    He wants to check periodically and as new data is added. Similar to Alice:

      - He knows what some of the columns mean, but not all - and there are MANY of them (only a subset currently shown in examples and fixtures).

      - He can assume that all batches share the same structure.

      - There are some columns which share characteristics so he would like to apply the same set of expectations to them.

    But he also:

      - Wants to verify that the latest batches are not so different from all of the batches last year, if they are he wants to be alerted as maybe there are data collection issues or fares have changed significantly.

    Bob configures his Profiler using the yaml configurations and data file locations captured in this fixture.
    """

    with open("bob_user_workflow_verbose_profiler_config.yml") as f:
        verbose_profiler_config = f.read()

    profiler_configs: List[str] = []
    profiler_configs.append(verbose_profiler_config)

    return {"profiler_configs": profiler_configs}
