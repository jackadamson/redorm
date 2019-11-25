from dataclasses_jsonschema import JsonSchemaMixin, FieldEncoder, JsonDict
from typing import NewType
from datetime import datetime
from base64 import b64decode, b64encode

Binary = NewType("Binary", bytes)
DateTime = NewType("DateTime", datetime)


class BinaryField(FieldEncoder):
    """Base64 encoded bytes"""

    def to_wire(self, value: Binary) -> str:
        return b64encode(value).decode("utf-8")

    def to_python(self, value: str) -> Binary:
        return b64decode(value)

    @property
    def json_schema(self) -> JsonDict:
        return {
            "type": "string",
            "pattern": r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
        }


JsonSchemaMixin.register_field_encoders({Binary: BinaryField()})


class DateTimeField(FieldEncoder):
    """Wrapper around Python datetime.datetime object"""

    def to_wire(self, value: DateTime) -> float:
        if isinstance(value, float):
            return value
        return value.timestamp()

    def to_python(self, value: float) -> DateTime:
        return DateTime(datetime.fromtimestamp(value))

    @property
    def json_schema(self) -> JsonDict:
        return {"type": "number"}


JsonSchemaMixin.register_field_encoders({DateTime: DateTimeField()})
