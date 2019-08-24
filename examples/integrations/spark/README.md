This Spark integration is provided as an example. Using it will also introduce dependency on the requirements listed in
the "requirements_spark.txt" file in the "great_expectations/examples/integrations/spark" directory.

Please make sure that the file "great_expectations/examples/data/Titanic.csv" exists.

To run the example:

* change directories into "great_expectations/examples/integrations/spark" and then execute
* pip install -r requirements_spark.txt
* pytest tests/test_titanic_csv_pieline.py

on the command line.  The result should be that both validations (before and after processing the data sets) pass.
