from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class ValidationOperatorAnonymizer(BaseAnonymizer):
    pass

    # def _anonymize_validation_operator_info(
    #     self,
    #     validation_operator_name: str,
    #     validation_operator_obj: object,
    # ) -> dict:
    #     """Anonymize ValidationOperator objs from the 'great_expectations.validation_operators' module.

    #     Args:
    #         validation_operator_name (str): The name of the operator.
    #         validation_operator_obj (object): An instance of the operator base class or one of its children.

    #     Returns:
    #         An anonymized dictionary payload that obfuscates user-specific details.
    #     """
    #     anonymized_info_dict: dict = {
    #         "anonymized_name": self._anonymize_string(validation_operator_name)
    #     }
    #     actions_dict: dict = validation_operator_obj.actions

    #     anonymized_info_dict.update(
    #         self._anonymize_object_info(
    #             object_=validation_operator_obj,
    #             anonymized_info_dict=anonymized_info_dict,
    #         )
    #     )

    #     if actions_dict:
    #         anonymized_info_dict["anonymized_action_list"] = [
    #             self._anonymize_action_info(
    #                 action_name=action_name, action_obj=action_obj
    #             )
    #             for action_name, action_obj in actions_dict.items()
    #         ]

    #     return anonymized_info_dict
