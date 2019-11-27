from typing import List, Type, Optional, TypeVar, Union
from enum import Enum, auto
from redorm.model import RedormBase, all_models, IRelationship
from redorm.client import red

__all__ = [
    "RelationshipConfigEnum",
    "Relationship",
    "one_to_many",
    "one_to_one",
    "many_to_one",
    "many_to_many",
]


class RelationshipConfigEnum(Enum):
    ONE_TO_MANY = auto()
    ONE_TO_ONE = auto()
    MANY_TO_ONE = auto()
    MANY_TO_MANY = auto()


T = TypeVar("T", bound=RedormBase)
U = TypeVar("U", bound=RedormBase)


class Relationship(IRelationship):
    def __init__(
        self,
        foreign_type: Union[str, Type[U]],
        config: RelationshipConfigEnum = RelationshipConfigEnum.MANY_TO_MANY,
        backref: Optional[str] = None,
    ):
        self.config = config
        if isinstance(foreign_type, str):
            self.foreign_type = None
            self.__foreign_type = foreign_type
        else:
            self.foreign_type = foreign_type
            self.__foreign_type = foreign_type.__name__
        self.fdel = None
        self.backref = backref
        self.__relationship_name = None
        self.__doc__ = None
        self.__owner = None

    def __set_name__(self, owner, name):
        self.__owner = owner
        self.__relationship_name = name

    def get_foreign_type(self) -> Type[U]:
        if self.foreign_type is None:
            self.foreign_type = all_models[self.__foreign_type]
            return self.foreign_type
        else:
            return self.foreign_type

    def __get__(self, instance: T, objtype=None):
        if instance is None:
            return self
        foreign_type: Type[U] = self.get_foreign_type()
        relationship_path = f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
        if self.config in {
            RelationshipConfigEnum.MANY_TO_MANY,
            RelationshipConfigEnum.ONE_TO_MANY,
        }:
            related_ids = red.client.smembers(relationship_path)
            return foreign_type.get_bulk(related_ids)
        else:
            related_id = red.client.get(relationship_path)
            return foreign_type.get(related_id) if related_id is not None else None

    def __set__(
        self, instance: T, value: Union[None, str, U, List[Union[str, U]]],
    ):
        foreign_type = self.get_foreign_type()
        relationship_path = f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
        if self.config in {
            RelationshipConfigEnum.MANY_TO_ONE,
            RelationshipConfigEnum.ONE_TO_ONE,
        }:
            related_id_new: Optional[str]
            if isinstance(value, RedormBase):
                related_id_new = value.id
            elif isinstance(value, str):
                related_id_new = value
            elif value is None:
                related_id_new = None
            else:
                raise ValueError("Expected new value of string or Model")
            if related_id_new is None:
                pipeline = red.client.pipeline()
                pipeline.get(relationship_path)
                pipeline.delete(relationship_path)
                result = pipeline.execute()
                related_id_old = result[0]
            else:
                related_id_old = red.client.getset(relationship_path, related_id_new,)
            if related_id_new == related_id_old or self.backref is None:
                return
            rel_new = (
                f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_new}"
            )
            rel_old = (
                f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_old}"
            )
            if self.config == RelationshipConfigEnum.MANY_TO_ONE:
                if related_id_old is None:
                    red.client.sadd(
                        rel_new, instance.id,
                    )
                elif related_id_new is None:
                    red.client.srem(
                        rel_old, instance.id,
                    )
                else:
                    red.client.smove(
                        rel_old, rel_new, instance.id,
                    )
            else:
                if related_id_old is None:
                    red.client.set(
                        rel_new, instance.id,
                    )
                elif related_id_new is None:
                    red.client.delete(rel_new,)
                else:
                    red.client.rename(
                        rel_old, rel_new,
                    )
        else:
            if (not isinstance(value, list)) and (not isinstance(value, set)):
                raise ValueError("Expected list or set for new relationships")
            old_related_ids = {r for r in red.client.smembers(relationship_path)}
            new_related_ids = {
                (r.id if isinstance(r, RedormBase) else r) for r in value
            }
            if len(old_related_ids.symmetric_difference(new_related_ids)) == 0:
                return
            pipeline = red.client.pipeline()
            pipeline.delete(relationship_path)
            if new_related_ids:
                pipeline.sadd(
                    relationship_path, *new_related_ids,
                )
            if self.backref is None:
                pipeline.execute()
                return
            ids_to_remove = old_related_ids - new_related_ids
            ids_to_add = new_related_ids - old_related_ids
            reverse_path = f"{foreign_type.__name__}:relationship:{self.backref}"
            if self.config == RelationshipConfigEnum.MANY_TO_MANY:
                for idr in ids_to_remove:
                    pipeline.srem(
                        f"{reverse_path}:{idr}", instance.id,
                    )
                for ida in ids_to_add:
                    pipeline.sadd(
                        f"{reverse_path}:{ida}", instance.id,
                    )
            elif self.config == RelationshipConfigEnum.ONE_TO_MANY:
                for idr in ids_to_remove:
                    pipeline.delete(f"{reverse_path}:{idr}")
                for ida in ids_to_add:
                    pipeline.set(
                        f"{reverse_path}:{ida}", instance.id,
                    )
            else:
                raise ValueError(
                    "Expected relationship config to be of type RelationshipConfigEnum"
                )
            pipeline.execute()


def one_to_many(
    foreign_type: Union[str, Type[U]], backref: Optional[str] = None,
):
    return Relationship(
        foreign_type=foreign_type,
        config=RelationshipConfigEnum.ONE_TO_MANY,
        backref=backref,
    )


def one_to_one(
    foreign_type: Union[str, Type[U]], backref: Optional[str] = None,
):
    return Relationship(
        foreign_type=foreign_type,
        config=RelationshipConfigEnum.ONE_TO_ONE,
        backref=backref,
    )


def many_to_one(
    foreign_type: Union[str, Type[U]], backref: Optional[str] = None,
):
    return Relationship(
        foreign_type=foreign_type,
        config=RelationshipConfigEnum.MANY_TO_ONE,
        backref=backref,
    )


def many_to_many(
    foreign_type: Union[str, Type[U]], backref: Optional[str] = None,
):
    return Relationship(
        foreign_type=foreign_type,
        config=RelationshipConfigEnum.MANY_TO_MANY,
        backref=backref,
    )
