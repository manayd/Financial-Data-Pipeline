from typing import TypeVar

import orjson
from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class KafkaSerializer:
    @staticmethod
    def serialize(event: BaseModel) -> bytes:
        return orjson.dumps(event.model_dump(mode="json"))


class KafkaDeserializer:
    @staticmethod
    def deserialize(data: bytes, model_class: type[T]) -> T:
        payload = orjson.loads(data)
        return model_class.model_validate(payload)
