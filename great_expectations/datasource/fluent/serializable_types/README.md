# Fluent Serializable Types

This folder contains custom types used by various datasources e.g. Spark that do not have a readily serializable version.

To implement a new serializable type, follow the pattern set in SerializableStructType where:

1. `__init__()`: instantiate an instance variable with an actual instance of the type.
2. `__init__()`: serialized version stored in the data of the object (subclassing dict in the case of SerializableStructType) using the instance variable.
3. `validate()`: classmethod which validates and converts to a SerializableStructType, returning the SerializableStructType. 

Why do we do this? These custom types can be used in pydantic models for data assets, and when the asset is serialized these types will handle serializing the model fields they are applied to.
