from __future__ import annotations

from great_expectations.compatibility.pyspark import pyspark


class SerializableStructType(dict):
    """Custom type implementing pydantic validation."""

    struct_type: pyspark.sql.types.StructType

    def __init__(
        self,
        fields_or_struct_type: pyspark.sql.types.StructType
        | list[pyspark.sql.types.StructField]
        | None,
    ):
        if isinstance(fields_or_struct_type, pyspark.sql.types.StructType):
            self.struct_type = fields_or_struct_type
        else:
            self.struct_type = pyspark.sql.types.StructType(
                fields=fields_or_struct_type
            )

        json_value = self.struct_type.jsonValue()
        super().__init__(**json_value)

    @classmethod
    def validate(
        cls,
        fields_or_struct_type: pyspark.sql.types.StructType
        | list[pyspark.sql.types.StructField]
        | None,
    ):
        """If already StructType then return otherwise try to create a StructType."""
        if isinstance(fields_or_struct_type, pyspark.sql.types.StructType):
            json_value = fields_or_struct_type.jsonValue()
            return cls(**json_value)
        else:
            return cls(fields_or_struct_type)

    @classmethod
    def __get_validators__(cls):
        # one or more validators may be yielded which will be called in the
        # order to validate the input, each validator will receive as an input
        # the value returned from the previous validator
        yield cls.validate
