from great_expectations import get_context

context = get_context(project_root_dir="./gx")
result = context.checkpoints.get("project_integration_checkpoint").run(
    batch_parameters={"year": "2020", "month": "04"}
)
print(result)
