from typing import List, Type, Optional, TypeVar
from dataclasses import dataclass, field
from uuid import uuid4
import json
import redis
from dataclasses_jsonschema import JsonSchemaMixin
from redorm.exceptions import RedisInstanceNotFound

redis_pool = redis.ConnectionPool(host="localhost", port=6379, db=1)
redis_client = redis.Redis(connection_pool=redis_pool)

S = TypeVar("S", bound="RedisBase")

all_models = dict()


@dataclass
class RedisBase(JsonSchemaMixin):
    id: str = field()

    @classmethod
    def get(cls: Type[S], instance_id: str) -> Optional[S]:
        data = redis_client.get(f"{cls.__name__}:member:{instance_id}")
        if data is None:
            return None
        else:
            return cls.from_dict(json.loads(data.decode("utf-8")))

    @classmethod
    def get_bulk(cls: Type[S], instance_ids: List[str]) -> List[S]:
        data = redis_client.mget(
            [f"{cls.__name__}:member:{instance_id}" for instance_id in instance_ids]
        )
        if data is None:
            return []
        else:
            return [
                cls.from_dict(json.loads(d.decode("utf-8")))
                for d in data
                if d is not None
            ]

    @classmethod
    def create(cls: Type[S], **kwargs) -> S:
        non_relations = {k: v for k, v in kwargs.items() if k in cls.__annotations__}
        new_instance = cls(id=str(uuid4()), **non_relations)
        for k, v in kwargs.items():
            if k not in cls.__annotations__:
                setattr(new_instance, k, v)

        pipe = redis_client.pipeline()
        instance_dict = new_instance.to_dict()
        pipe.set(
            f"{cls.__name__}:member:{new_instance.id}",
            json.dumps(instance_dict, sort_keys=True),
        )
        pipe.execute()
        return new_instance

    @classmethod
    def list(cls: Type[S]) -> List[S]:
        members_data = redis_client.mget(
            list(redis_client.scan_iter(f"{cls.__name__}:member:*"))
        )
        return [
            cls.from_json(member_data.decode("utf-8"))
            for member_data in members_data
            if member_data
        ]

    def delete(self):
        with redis_client.lock(f"{self.__class__.__name__}:member:{self.id}"):
            redis_client.delete(self.id)

    def refresh(self) -> None:
        latest = redis_client.get(f"{self.__class__.__name__}:member:{self.id}")
        if latest is None:
            raise RedisInstanceNotFound
        for k, v in json.loads(latest.decode("utf-8")):
            setattr(self, k, v)

    def save(self) -> None:
        redis_client.set(
            f"{self.__class__.__name__}:member:{self.id}",
            json.dumps(self.to_dict(), sort_keys=True),
        )

    def update(self, **kwargs):
        with redis_client.lock(f"{self.__class__.__name__}:member:{self.id}"):
            self.refresh()
            for k, v in kwargs:
                setattr(self, k, v)
            redis_client.set(
                f"{self.__class__.__name__}:member:{self.id}",
                json.dumps(self.to_dict(), sort_keys=True),
            )

    def __init_subclass__(cls, **kwargs):
        all_models[cls.__name__] = cls
        super().__init_subclass__(**kwargs)
