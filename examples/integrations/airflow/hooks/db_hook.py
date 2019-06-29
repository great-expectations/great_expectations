import great_expectations as ge
from airflow.hooks.mysql_hook import MySqlHook

####
#
# NOTE: this code has not been updated for the new GE 0.7 naming conventions
#
####

class ExpectationMySQLHook(MySqlHook):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_ge_df(self, dataset_name, **kwargs):
        self.log.info("Connecting to dataset {dataset} on {uri}".format(uri=self.get_uri(), dataset=dataset_name))
        sql_context = ge.get_data_context('SqlAlchemy', self.get_uri())

        return sql_context.get_dataset(dataset_name=dataset_name, **kwargs)
