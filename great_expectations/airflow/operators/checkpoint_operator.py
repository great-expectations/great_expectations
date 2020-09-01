import logging

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from great_expectations import DataContext


class CheckpointOperator(BaseOperator):
    template_fields = ['checkpoint', 'run_id']

    @apply_defaults
    def __init__(self, context_root_dir, checkpoint, run_id: dict, **kwargs):
        super().__init__(**kwargs)

        self.ge_context = DataContext(context_root_dir)
        self.checkpoint = self.ge_context.get_checkpoint(checkpoint)
        self.run_id = run_id

    def execute(self, context):
        batches_to_validate = self.load_batches()

        logging.info('Executing batches')
        logging.info(str(batches_to_validate))
        logging.info(batches_to_validate)

        results = self.ge_context.run_validation_operator(
            self.checkpoint["validation_operator_name"],
            assets_to_validate=batches_to_validate,
            run_id=self.run_id
        )

        # take action based on results
        if not results["success"]:
            print(results)
            raise AirflowException('Validation failed')

        print("Validation Succeeded!")

    def load_batches(self):
        batches_to_validate = []
        for batch in self.checkpoint["batches"]:
            batch_kwargs = batch["batch_kwargs"]
            for suite_name in batch["expectation_suite_names"]:
                suite = self.ge_context.get_expectation_suite(suite_name)
                batch = self.ge_context.get_batch(batch_kwargs, suite)
                batches_to_validate.append(batch)

        return batches_to_validate
