from typing import List, Type, TypeVar, Final, Set
from dataclasses import dataclass, field, fields, Field
from uuid import uuid4
import json
from dataclasses_jsonschema import JsonSchemaMixin
from redorm.exceptions import (
    InstanceNotFound,
    UniqueContstraintViolation,
    UnknownFieldName,
    FilterOnUnindexedField,
    MultipleInstancesReturned,
)
from redorm.client import red

S = TypeVar("S", bound="RedormBase")

all_models = dict()


@dataclass
class RedormBase(JsonSchemaMixin):
    id: str = field(metadata={"unique": True})

    @classmethod
    def get(cls: Type[S], instance_id=None, **kwargs) -> S:
        if instance_id is not None and len(kwargs) == 0:
            return cls._get(instance_id)
        else:
            if instance_id is None:
                instance_ids = cls._list_ids(**kwargs)
            else:
                instance_ids = cls._list_ids(id=instance_id, **kwargs)

            if len(instance_ids) == 1:
                return cls._get(instance_ids.pop())
            elif len(instance_ids) > 1:
                raise MultipleInstancesReturned
            else:
                raise InstanceNotFound

    @classmethod
    def _get(cls: Type[S], instance_id: str) -> S:
        data = red.client.get(f"{cls.__name__}:member:{instance_id}")
        if data is None:
            raise InstanceNotFound
        else:
            return cls.from_json(data, validate=False)

    @classmethod
    def get_bulk(cls: Type[S], instance_ids: List[str]) -> List[S]:
        data = red.client.mget(
            [f"{cls.__name__}:member:{instance_id}" for instance_id in instance_ids]
        )
        if data is None:
            return []
        else:
            return [cls.from_dict(json.loads(d)) for d in data if d is not None]

    @classmethod
    def create(cls: Type[S], **kwargs) -> S:
        # Handle non-relationship parts
        instance_fields = fields(cls)
        field_values = {
            f.name: kwargs.get(f.name) for f in instance_fields if f.name in kwargs
        }
        new_id = str(uuid4())
        new_instance = cls.from_dict(dict(id=new_id, **field_values))
        new_instance.save()
        for k, v in kwargs.items():
            if k not in field_values:
                setattr(new_instance, k, v)
        return new_instance

    @classmethod
    def _list_ids(cls, **kwargs) -> Set[str]:
        field_dict = {f.name: f for f in fields(cls)}
        pre_pipeline = red.client.pipeline()
        try:
            for k, v in kwargs.items():
                if k in field_dict:
                    f: Field = field_dict[k]
                    if field_dict[k].metadata.get("unique"):
                        if v is None:
                            pre_pipeline.smembers(f"{cls.__name__}:keynull:{f.name}")
                        else:
                            pre_pipeline.hget(
                                f"{cls.__name__}:key:{k}",
                                cls._encode_field(f.type, v, omit_none=False),
                            )
                    elif field_dict[k].metadata.get("index"):
                        if v is None:
                            key = f"{cls.__name__}:indexnull:{f.name}"
                        else:
                            key = f"{cls.__name__}:index:{f.name}:{cls._encode_field(f.type, v, omit_none=False)}"
                        pre_pipeline.smembers(key)
                    else:
                        raise FilterOnUnindexedField(
                            f"Trying to filter on unindexed field: {k}"
                        )
                elif isinstance(cls.__dict__[k], IRelationship):
                    raise NotImplementedError(
                        "Can't filter based off relationships yet"
                    )

        except KeyError as e:
            raise UnknownFieldName(*e.args) from e
        results = pre_pipeline.execute()
        sets = [r if isinstance(r, set) else {r} for r in results]
        ret = sets[0]
        for s in sets[1:]:
            ret.intersection_update(s)
        return ret

    @classmethod
    def list(cls: Type[S], **kwargs) -> List[S]:
        if len(kwargs) > 0:
            member_ids = cls._list_ids(**kwargs)
            return cls.get_bulk(list(member_ids))
        else:
            members_data = red.client.mget(
                list(red.client.scan_iter(f"{cls.__name__}:member:*"))
            )
        return [
            cls.from_json(member_data) for member_data in members_data if member_data
        ]

    def delete(self):
        # TODO: Handle removal of relationships and unique/indexes
        with red.client.lock(f"{self.__class__.__name__}:lock:{self.id}"):
            red.client.delete(self.id)

    def refresh(self) -> None:
        latest = red.client.get(f"{self.__class__.__name__}:member:{self.id}")
        if latest is None:
            raise InstanceNotFound
        for k, v in json.loads(latest).items():
            setattr(self, k, v)

    def save(self) -> None:
        old_json = red.client.get(f"{self.__class__.__name__}:member:{self.id}")
        if old_json is None:
            old_dict = {}
            new = True
        else:
            old = self.__class__.from_json(old_json)
            old_dict = old.to_dict(omit_none=False)
            new = False
        instance_dict = self.to_dict(omit_none=True)
        instance_fields = fields(self.__class__)
        cls_name = self.__class__.__name__

        # Pipeline to rollback changes on error
        revert_pipeline = red.client.pipeline()

        # Pipeline to ensure uniqueness of unique fields
        unique_pipeline = red.client.pipeline()
        changed_unique_fields_non_null = [
            f
            for f in instance_fields
            if f.metadata.get("unique")
            and instance_dict.get(f.name) is not None
            and (instance_dict.get(f.name) != old_dict.get(f.name) or new)
        ]
        changed_unique_fields_null = [
            f
            for f in instance_fields
            if f.metadata.get("unique")
            and instance_dict.get(f.name) is None
            and (instance_dict.get(f.name) != old_dict.get(f.name) or new)
        ]
        for f in changed_unique_fields_non_null:
            unique_pipeline.hget(f"{cls_name}:key:{f.name}", instance_dict[f.name])
            unique_pipeline.hsetnx(
                f"{cls_name}:key:{f.name}", instance_dict[f.name], self.id
            )
            if old_dict.get(f.name) is not None:
                revert_pipeline.hset(
                    f"{cls_name}:key:{f.name}", old_dict[f.name], self.id
                )
                unique_pipeline.ping()
            else:
                revert_pipeline.sadd(f"{cls_name}:keynull:{f.name}", self.id)
                unique_pipeline.srem(f"{cls_name}:keynull:{f.name}", self.id)

        for f in changed_unique_fields_null:
            unique_pipeline.sadd(f"{cls_name}:keynull:{f.name}", self.id)
            revert_pipeline.srem(f"{cls_name}:keynull:{f.name}", self.id)
        unique_pipeline_result = unique_pipeline.execute()
        unique_violating_fields = []
        for i, r in enumerate(
            unique_pipeline_result[: len(changed_unique_fields_non_null) * 3 : 3]
        ):
            if r is None:
                revert_pipeline.hdel(
                    f"{cls_name}:key:{changed_unique_fields_non_null[i].name}",
                    instance_dict[changed_unique_fields_non_null[i].name],
                )
            elif r != self.id:
                revert_pipeline.hdel(
                    f"{cls_name}:key:{changed_unique_fields_non_null[i].name}",
                    instance_dict[changed_unique_fields_non_null[i].name],
                )
                # Failed to set as key already exists
                unique_violating_fields.append(changed_unique_fields_non_null[i].name)

        try:
            if len(unique_violating_fields) > 0:
                raise UniqueContstraintViolation(
                    f"New object contained non-unique values: {unique_violating_fields!r}"
                )
            # Fields that require indexing and are not indexed by uniqueness
            # Index is stored as a set of ids
            indexed_non_unique_fields = [
                f
                for f in instance_fields
                if f.metadata.get("index")
                and not f.metadata.get("unique")
                and (instance_dict.get(f.name) != old_dict.get(f.name) or new)
            ]
            # Pipeline to index indexable fields
            index_pipeline = red.client.pipeline()
            for f in indexed_non_unique_fields:
                if instance_dict.get(f.name) is None:
                    index_pipeline.sadd(
                        f"{cls_name}:indexnull:{f.name}", self.id,
                    )
                    if new:
                        index_pipeline.ping()
                    else:
                        index_pipeline.srem(
                            f"{cls_name}:index:{f.name}:{self.__class__._encode_field(f.type, old_dict[f.name], omit_none=True)}",
                            self.id,
                        )
                        revert_pipeline.sadd(
                            f"{cls_name}:index:{f.name}:{self.__class__._encode_field(f.type, old_dict[f.name], omit_none=True)}",
                            self.id,
                        )
                else:
                    index_pipeline.sadd(
                        f"{cls_name}:index:{f.name}:{self.__class__._encode_field(f.type, instance_dict[f.name], omit_none=True)}",
                        self.id,
                    )
                    if new:
                        index_pipeline.ping()
                    elif old_dict.get(f.name) is None:
                        index_pipeline.srem(
                            f"{cls_name}:indexnull:{f.name}", self.id,
                        )
                        revert_pipeline.sadd(f"{cls_name}:indexnull:{f.name}", self.id)
                    else:
                        index_pipeline.srem(
                            f"{cls_name}:index:{f.name}:{self.__class__._encode_field(f.type, old_dict[f.name], omit_none=True)}",
                            self.id,
                        )
                        revert_pipeline.sadd(
                            f"{cls_name}:index:{f.name}:{self.__class__._encode_field(f.type, old_dict[f.name], omit_none=True)}",
                            self.id,
                        )

            index_pipeline_results = index_pipeline.execute()
            for i, r in enumerate(index_pipeline_results[::2]):
                if r == 1:
                    f = indexed_non_unique_fields[i]
                    if instance_dict.get(f.name) is None:
                        revert_pipeline.srem(
                            f"{cls_name}:indexnull:{f.name}", self.id,
                        )
                    else:
                        revert_pipeline.srem(
                            f"{cls_name}:index:{f.name}:{self.__class__._encode_field(f.type, instance_dict[f.name], omit_none=True)}",
                            self.id,
                        )

            pipe = red.client.pipeline()
            pipe.set(
                f"{cls_name}:member:{self.id}",
                json.dumps(instance_dict, sort_keys=True),
            )
            pipe.execute()
        except Exception as e:
            revert_pipeline.execute()
            raise e

    def update(self, **kwargs):
        with red.client.lock(f"{self.__class__.__name__}:lock:{self.id}"):
            self.refresh()
            for k, v in kwargs.items():
                setattr(self, k, v)
            self.save()

    def __init_subclass__(cls, **kwargs):
        all_models[cls.__name__] = cls
        super().__init_subclass__(**kwargs)


class IRelationship:
    pass
