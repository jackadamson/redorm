from typing import (
    List,
    Type,
    TypeVar,
    Set,
    ClassVar,
    Iterable,
    Callable,
    Optional,
)
from dataclasses import dataclass, field, fields, Field
from uuid import uuid4
from collections import OrderedDict
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


class Query:
    pipeline_results: Optional[List]

    def __init__(self):
        self.results = []
        self.pipeline_results = None
        self.resolvers: List[Callable[["Query"], None]] = []
        self.pipeline = red.client.pipeline()

    def execute(self) -> List:
        self.pipeline_results: List = self.pipeline.execute()
        results = []
        while self.resolvers:
            resolver = self.resolvers.pop()
            try:
                results.append(resolver(self))
            except InstanceNotFound:
                pass
        return list(reversed(results))


@dataclass
class RedormBase(JsonSchemaMixin):
    id: str = field(metadata={"unique": True})
    _relationships: ClassVar = OrderedDict()

    @classmethod
    def get(cls: Type[S], instance_id=None, **kwargs) -> S:
        if instance_id is not None and len(kwargs) == 0:
            query = Query()
            cls._get(query, instance_id)
            res = query.execute()
            if res:
                return res[0]
            else:
                raise InstanceNotFound
        else:
            if instance_id is None:
                if kwargs:
                    instance_ids = cls._list_ids(**kwargs)
                else:
                    raise InstanceNotFound
            else:
                instance_ids = cls._list_ids(id=instance_id, **kwargs)

            if len(instance_ids) == 1:
                instance_id = instance_ids.pop()
                query = Query()
                cls._get(query, instance_id)
                return query.execute()[0]
            elif len(instance_ids) > 1:
                raise MultipleInstancesReturned
            else:
                raise InstanceNotFound

    @classmethod
    def _resolve(cls: Type[S], query: Query) -> S:
        for rel_name, relation in reversed(cls._relationships.items()):
            relation.cached_ref = query.pipeline_results.pop()
            if not relation.lazy:
                res = query.pipeline_results.pop()
                if res is None:
                    relation.cached_class = None
                elif isinstance(res, list):
                    relation.cached_class = [
                        relation.get_foreign_type().from_json(d, validate=False) for d in res if d is not None
                    ]
                else:
                    relation.cached_class = relation.get_foreign_type().from_json(res, validate=False)
        data = query.pipeline_results.pop()
        if data is None:
            print(
                f"None for data, class={cls.__name__!r}, query.pipeline_results={query.pipeline_results!r}, query.resolvers={query.resolvers!r}"
            )
            raise InstanceNotFound
        return cls.from_json(data, validate=False)

    @classmethod
    def _get(cls: Type[S], query: Query, instance_id: str):
        query.pipeline.get(f"{cls.__name__}:member:{instance_id}")
        for rel_name, relation in cls._relationships.items():
            rel_key = f"{cls.__name__}:relationship:{rel_name}:{instance_id}"
            if not relation.lazy:
                if relation.to_many:
                    query.pipeline.evalsha(
                        red.get_set_indirect_sha,
                        2,
                        f"{relation.get_foreign_type().__name__}:member:",
                        rel_key,
                    )
                else:
                    query.pipeline.evalsha(
                        red.get_key_indirect_sha,
                        1,
                        rel_key,
                    )
            if relation.to_many:
                query.pipeline.smembers(rel_key)
            else:
                query.pipeline.get(rel_key)

        query.resolvers.append(cls._resolve)

    @classmethod
    def get_bulk(cls: Type[S], instance_ids: Set[str]) -> Iterable[S]:
        if len(instance_ids) == 0:
            return []
        query = Query()
        for instance_id in instance_ids:
            cls._get(query, instance_id)
        return query.execute()

    @classmethod
    def create(cls: Type[S], **kwargs) -> S:
        # Handle non-relationship parts
        instance_fields = fields(cls)
        field_values = {f.name: kwargs.get(f.name) for f in instance_fields if f.name in kwargs}
        new_id = str(uuid4())
        new_instance = cls.from_dict(dict(id=new_id, **field_values))
        for k, v in kwargs.items():
            if k not in field_values:
                setattr(new_instance, k, v)
        new_instance.save()
        return new_instance

    @classmethod
    def _list_ids(cls, **kwargs) -> Set[str]:
        field_dict = {f.name: f for f in fields(cls)}
        pre_pipeline = red.client.pipeline()
        indexes = set()
        if not kwargs:
            return set()
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
                            indexes.add(f"{cls.__name__}:indexnull:{f.name}")
                        else:
                            indexes.add(
                                f"{cls.__name__}:index:{f.name}:{cls._encode_field(f.type, v, omit_none=False)}"
                            )
                    else:
                        raise FilterOnUnindexedField(f"Trying to filter on unindexed field: {k}")
                elif isinstance(cls.__dict__[k], IRelationship):
                    rel = cls.__dict__[k]
                    if rel.to_many:
                        raise NotImplementedError("Can't filter based off to-many relationships")
                    if rel.backref is None:
                        raise NotImplementedError("Filtering on a relationship requires a backref")
                    rel_type = rel.get_foreign_type().__name__
                    if not isinstance(v, str):
                        v = v.id
                    ref = f"{rel_type}:relationship:{rel.backref}:{v}"
                    if rel.many_to:
                        indexes.add(ref)
                    else:
                        pre_pipeline.get(ref)

        except KeyError as e:
            raise UnknownFieldName(*e.args) from e
        if indexes:
            pre_pipeline.sinter(indexes)
        results = pre_pipeline.execute()
        sets = [r if isinstance(r, set) else {r} for r in results if r is not None]
        if not sets:
            return set()
        ret = sets[0]
        for s in sets[1:]:
            ret.intersection_update(s)
        return ret

    @classmethod
    def list(cls: Type[S], **kwargs) -> Iterable[S]:
        if len(kwargs) > 0:
            member_ids = cls._list_ids(**kwargs)
        else:
            member_ids = red.client.smembers(f"{cls.__name__}:all")
        return cls.get_bulk(member_ids)

    def lock(
        self,
        timeout=None,
        sleep=0.1,
        blocking_timeout=None,
        lock_class=None,
        thread_local=True,
    ):
        return red.client.lock(
            f"{self.__class__.__name__}:userlock:{self.id}",
            timeout=timeout,
            sleep=sleep,
            lock_class=lock_class,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

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
        unique_pipeline.sadd(f"{self.__class__.__name__}:all", self.id)
        # TODO: Handle rollback
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
            unique_pipeline.hsetnx(f"{cls_name}:key:{f.name}", instance_dict[f.name], self.id)
            if old_dict.get(f.name) is not None:
                revert_pipeline.hset(f"{cls_name}:key:{f.name}", old_dict[f.name], self.id)
                unique_pipeline.ping()
            else:
                revert_pipeline.sadd(f"{cls_name}:keynull:{f.name}", self.id)
                unique_pipeline.srem(f"{cls_name}:keynull:{f.name}", self.id)

        for f in changed_unique_fields_null:
            unique_pipeline.sadd(f"{cls_name}:keynull:{f.name}", self.id)
            revert_pipeline.srem(f"{cls_name}:keynull:{f.name}", self.id)
        unique_pipeline_result_all = unique_pipeline.execute()
        unique_pipeline_result = unique_pipeline_result_all[1:]
        if unique_pipeline_result_all[0] == 1:
            revert_pipeline.srem(f"{self.__class__.__name__}:all", self.id)
        unique_violating_fields = []
        for i, r in enumerate(unique_pipeline_result[: len(changed_unique_fields_non_null) * 3 : 3]):
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
                raise UniqueContstraintViolation(f"New object contained non-unique values: {unique_violating_fields!r}")
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
                        f"{cls_name}:indexnull:{f.name}",
                        self.id,
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
                            f"{cls_name}:indexnull:{f.name}",
                            self.id,
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
                            f"{cls_name}:indexnull:{f.name}",
                            self.id,
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
