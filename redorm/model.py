import json
from collections import OrderedDict, namedtuple
from dataclasses import dataclass, field, fields, Field
from typing import (
    List,
    Type,
    TypeVar,
    Set,
    ClassVar,
    Callable,
    Optional,
)
from uuid import uuid4

from dataclasses_jsonschema import JsonSchemaMixin
from redis import ResponseError
from redis.lock import Lock

from redorm.client import red
from redorm.exceptions import (
    InstanceNotFound,
    UniqueContstraintViolation,
    UnknownFieldName,
    FilterOnUnindexedField,
    MultipleInstancesReturned,
)

S = TypeVar("S", bound="RedormBase")

all_models = dict()
FieldChange = namedtuple("FieldChange", ["name", "old", "new"])


class Query:
    pipeline_results: Optional[List]

    def __init__(self):
        self.results = []
        self.pipeline_results = None
        self.resolvers: List[Callable[["Query"], S]] = []
        self.pipeline = red.client.pipeline()

    def execute(self) -> List:
        self.pipeline_results: List = self.pipeline.execute()
        results: List = []
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
        assert query.pipeline_results is not None
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
                        red.get_set_indirect_script.sha,
                        2,
                        f"{relation.get_foreign_type().__name__}:member:",
                        rel_key,
                    )
                else:
                    query.pipeline.evalsha(
                        red.get_key_indirect_script.sha,
                        1,
                        rel_key,
                    )
            if relation.to_many:
                query.pipeline.smembers(rel_key)
            else:
                query.pipeline.get(rel_key)

        query.resolvers.append(cls._resolve)

    @classmethod
    def get_bulk(cls: Type[S], instance_ids: Set[str]) -> List[S]:
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
    def list(cls: Type[S], **kwargs) -> List[S]:
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
    ) -> Lock:
        return red.client.lock(
            f"{self.__class__.__name__}:userlock:{self.id}",
            timeout=timeout,
            sleep=sleep,
            lock_class=lock_class,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    def delete(self):
        # TODO: Handle removal of relationships
        instance_id = self.id
        cls_name = self.__class__.__name__
        with red.client.lock(f"{cls_name}:lock:{instance_id}"):
            self.refresh()
            instance_dict = self.to_dict(omit_none=True)
            p = red.client.pipeline()
            p.srem(f"{cls_name}:all", instance_id)
            p.unlink(f"{cls_name}:member:{instance_id}")
            for f in fields(self.__class__):
                val = instance_dict.get(f.name)
                if f.metadata.get("unique"):
                    if val is None:
                        p.srem(f"{cls_name}:keynull:{f.name}", instance_id)
                    else:
                        p.hdel(f"{cls_name}:key:{f.name}", val)
                elif f.metadata.get("index"):
                    if val is None:
                        p.srem(f"{cls_name}:indexnull:{f.name}", instance_id)
                    else:
                        p.srem(
                            f"{cls_name}:index:{f.name}:{self.__class__._encode_field(f.type, instance_dict[f.name], omit_none=True)}",
                            self.id,
                        )
            p.execute(raise_on_error=True)

    def refresh(self) -> None:
        latest = red.client.get(f"{self.__class__.__name__}:member:{self.id}")
        if latest is None:
            raise InstanceNotFound
        for k, v in json.loads(latest).items():
            setattr(self, k, v)
        for k, v in self._relationships.items():
            v.cached_class = None
            v.cached_ref = None

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
        key_changes: List[FieldChange] = [
            FieldChange(f.name, old_dict.get(f.name), instance_dict.get(f.name))
            for f in instance_fields
            if f.metadata.get("unique") and (new or instance_dict.get(f.name) != old_dict.get(f.name))
        ]
        index_changes: List[FieldChange] = [
            FieldChange(f.name, old_dict.get(f.name), instance_dict.get(f.name))
            for f in instance_fields
            if not f.metadata.get("unique")
            and f.metadata.get("index")
            and (new or instance_dict.get(f.name) != old_dict.get(f.name))
        ]
        data = json.dumps(instance_dict, sort_keys=True)
        try:
            self._atomic_unique_save(key_changes=key_changes, index_changes=index_changes, data=data)
        except ResponseError as e:
            raise UniqueContstraintViolation(*e.args) from e

    def update(self, **kwargs):
        with red.client.lock(f"{self.__class__.__name__}:lock:{self.id}"):
            self.refresh()
            for k, v in kwargs.items():
                setattr(self, k, v)
            self.save()

    def _atomic_unique_save(self, key_changes: List[FieldChange], index_changes: List[FieldChange], data: str):
        unique = [change for change in key_changes if change.new is not None]
        unique_null = [change for change in key_changes if change.new is None]
        index = [change for change in index_changes if change.new is not None]
        index_null = [change for change in index_changes if change.new is None]
        args = [
            len(unique),
            len(unique_null),
            len(index),
            len(index_null),
            data,
            self.id,
            self.__class__.__name__,
            *[elem for change in unique for elem in change],
            *[elem for change in unique_null for elem in change],
            *[elem for change in index for elem in change],
            *[elem for change in index_null for elem in change],
        ]
        args_no_none = ["" if arg is None else arg for arg in args]
        red.unique_save(0, *args_no_none)

    def __init_subclass__(cls, **kwargs):
        all_models[cls.__name__] = cls
        super().__init_subclass__(**kwargs)

    def __hash__(self):
        return hash(f"{self.__class__.__name__}:{self.id}")


class IRelationship:
    pass
