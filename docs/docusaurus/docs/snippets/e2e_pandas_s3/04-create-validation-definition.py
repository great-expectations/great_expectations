from great_expectations import get_context

# TODO: will become from great_expectations import ValidationDefinition
from great_expectations.core import ValidationDefinition
from great_expectations.exceptions import DataContextError

context = get_context(project_root_dir="./")

# TODO: will become
# batch_definition = context.data_sources.get("project_name").get_asset("my_project").get_batch_definition("monthly")
batch_definition = (
    context.datasources["project_name"]
    .get_asset("my_project")
    .get_batch_definition("monthly")
)
suite = context.suites.get("project_name")

try:
    validation_definition = context.validation_definitions.get("my_project")
# TODO: will become except ResourceNotFoundError:
except DataContextError:
    # TODO: will become
    # validation_definition = context.validation_definitions.add(
    # name="my_project",
    # data=batch_definition,
    # suite=suite
    # )
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(name="my_project", data=batch_definition, suite=suite)
    )

### To run this in your project it is critical to provide batch_parameters
result = context.validation_definitions.get("my_project").run(
    batch_parameters={"year": "2020", "month": "04"}
)

# TODO: This should only run on the latest batch, or it should fail entirely with an error that
# batch parameters are missing
# result = context.validation_definitions.get("my_project").run()
print(result)
