This Spark integration is provided as an example. Using it will also introduce dependency on the requirements listed in
"requirements_spark.txt" in the main "great_expectations" distribution directory.

Please make sure that the file "examples/data/Titanic.csv" exists relative to the main "great_expectations" distribution
directory.

To run the example, change directories into "great_expectations/examples/integrations/spark" and then execute

pytest tests/test_titanic_csv_pieline.py

on the command line.  The result should be that both validations (before and after processing the data sets) pass.
