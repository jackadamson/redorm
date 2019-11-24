from dataclasses_jsonschema import JsonSchemaMixin, FieldEncoder, JsonDict
from typing import NewType
from base64 import b64decode, b64encode

Binary = NewType("Binary", bytes)


class BinaryField(FieldEncoder):
    """Base64 encoded bytes"""

    def to_wire(self, value: Binary) -> str:
        return b64encode(value).encode("utf-8")

    def to_python(self, value: str) -> Binary:
        return b64decode(value)

    @property
    def json_schema(self) -> JsonDict:
        return {"type": "string"}


JsonSchemaMixin.register_field_encoders({Binary: BinaryField()})
