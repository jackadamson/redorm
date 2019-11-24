from typing import List, Type, Optional, TypeVar, Union
from enum import Enum, auto
from redorm.model import RedisBase, all_models
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


T = TypeVar("T", bound=RedisBase)
U = TypeVar("U", bound=RedisBase)


class Relationship:
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

    def ensure_relationship_name(self, instance: T):
        if self.__relationship_name is None:
            rels = [k for k, v in instance.__class__.__dict__.items() if v is self]
            if len(rels) == 1:
                self.__relationship_name = rels[0]
            elif len(rels) > 1:
                raise ValueError("Same relationship re-used on object")
            else:
                raise ValueError("Could not find relationship on object")

    def get_foreign_type(self) -> Type[U]:
        if self.foreign_type is None:
            self.foreign_type = all_models[self.__foreign_type]
            return self.foreign_type
        else:
            return self.foreign_type

    def fget(self, instance: T):
        self.ensure_relationship_name(instance)
        foreign_type: Type[U] = self.get_foreign_type()
        if self.config in {
            RelationshipConfigEnum.MANY_TO_MANY,
            RelationshipConfigEnum.ONE_TO_MANY,
        }:
            related_ids = red.client.smembers(
                f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
            )
            return foreign_type.get_bulk([rid.decode() for rid in related_ids])
        else:
            related_id = red.client.get(
                f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
            )
            return (
                foreign_type.get(related_id.decode())
                if related_id is not None
                else None
            )

    def fset(
        self, instance: T, value: Union[None, str, U, List[Union[str, U]]],
    ):
        self.ensure_relationship_name(instance)
        foreign_type = self.get_foreign_type()
        if self.config in {
            RelationshipConfigEnum.MANY_TO_ONE,
            RelationshipConfigEnum.ONE_TO_ONE,
        }:
            related_id_new: Optional[str]
            if isinstance(value, RedisBase):
                related_id_new = value.id
            elif isinstance(value, str):
                related_id_new = value
            elif value is None:
                related_id_new = None
            else:
                raise ValueError("Expected new value of string or Model")
            if related_id_new is None:
                pipeline = red.client.pipeline()
                pipeline.get(
                    f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
                )
                pipeline.delete(
                    f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
                )
                result = pipeline.execute()
                print(f"results: {result!r}")
                related_id_old = result[0].decode() if result[0] else None
            else:
                related_id_old = red.client.getset(
                    f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}",
                    related_id_new,
                )
            if related_id_new == related_id_old or self.backref is None:
                return
            if self.config == RelationshipConfigEnum.MANY_TO_ONE:
                if related_id_old is None:
                    red.client.sadd(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_new}",
                        instance.id,
                    )
                elif related_id_new is None:
                    red.client.srem(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_old}",
                        instance.id,
                    )
                else:
                    red.client.smove(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_old}",
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_new}",
                        instance.id,
                    )
            else:
                if related_id_old is None:
                    red.client.set(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_new}",
                        instance.id,
                    )
                elif related_id_new is None:
                    red.client.delete(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_new}",
                    )
                else:
                    red.client.rename(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_old}",
                        f"{foreign_type.__name__}:relationship:{self.backref}:{related_id_new}",
                    )
        else:
            if (not isinstance(value, list)) and (not isinstance(value, set)):
                raise ValueError("Expected list or set for new relationships")
            old_related_ids = {
                r.decode()
                for r in red.client.smembers(
                    f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
                )
            }
            new_related_ids = {(r.id if isinstance(r, RedisBase) else r) for r in value}
            if len(old_related_ids.symmetric_difference(new_related_ids)) == 0:
                return
            pipeline = red.client.pipeline()
            pipeline.delete(
                f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}"
            )
            if new_related_ids:
                pipeline.sadd(
                    f"{instance.__class__.__name__}:relationship:{self.__relationship_name}:{instance.id}",
                    *new_related_ids,
                )
            if self.backref is None:
                pipeline.execute()
                return
            ids_to_remove = old_related_ids - new_related_ids
            ids_to_add = new_related_ids - old_related_ids
            if self.config == RelationshipConfigEnum.MANY_TO_MANY:
                for idr in ids_to_remove:
                    pipeline.srem(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{idr}",
                        instance.id,
                    )
                for ida in ids_to_add:
                    pipeline.sadd(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{ida}",
                        instance.id,
                    )
            elif self.config == RelationshipConfigEnum.ONE_TO_MANY:
                for idr in ids_to_remove:
                    pipeline.delete(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{idr}"
                    )
                for ida in ids_to_add:
                    pipeline.set(
                        f"{foreign_type.__name__}:relationship:{self.backref}:{ida}",
                        instance.id,
                    )
            else:
                raise ValueError(
                    "Expected relationship config to be of type RelationshipConfigEnum"
                )
            pipeline.execute()

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError("can't delete relationship")
        self.fdel(obj)

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.__doc__)

    def setter(self, fset):
        return type(self)(self.fget, fset, self.fdel, self.__doc__)

    def deleter(self, fdel):
        return type(self)(self.fget, self.fset, fdel, self.__doc__)


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
