from dataclasses import dataclass, field
from typing import Optional
from redorm import RedormBase, one_to_one, one_to_many, many_to_one, many_to_many, red

red.bind("redis://localhost")
red.client.flushall()


@dataclass
class Person(RedormBase):
    name: str = field(metadata={"unique": True})
    age: int = field(metadata={"index": True})
    evilness: Optional[int] = field(metadata={"index": True}, default=None)
    siblings = many_to_many(foreign_type="Person", backref="siblings")
    dad = many_to_one(foreign_type="Person", backref="children")
    children = one_to_many(foreign_type="Person", backref="dad")
    favourite_color = one_to_one("Color", backref="liker")


@dataclass
class Color(RedormBase):
    name: str
    liker = one_to_one(Person, backref="favourite_color")


homer = Person.create(name="Homer", age=50)
print(f"homer.children={homer.children!r}")
print(f"homer.favourite_color={homer.favourite_color!r}")
bart = Person.create(name="Bart", age=11, dad=homer)
evil_bart = Person.create(name="Evil Bart", age=11, dad=homer, evilness=100)
print(f"homer.children={homer.children!r}")
print(f"bart.favourite_color={bart.favourite_color!r}")
blue = Color.create(name="Blue", liker=bart)
print(f"bart.favourite_color={bart.favourite_color!r}")
print(f"blue.liker={blue.liker!r}")
lisa = Person.create(name="Lisa", age=9, dad=homer.id, siblings=[bart])
print(f"homer.children={homer.children!r}")
print(f"bart.dad={bart.dad!r}")
bart.dad = None
print(f"bart.dad={bart.dad!r}")
assert len(Person.list(evilness=None)) == 3
assert len(Person.list(evilness=100)) == 1
bart.update(evilness=100)
assert len(Person.list(evilness=None)) == 2
assert len(Person.list(evilness=100)) == 2
