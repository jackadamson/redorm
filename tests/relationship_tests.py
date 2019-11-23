from dataclasses import dataclass
from redorm import *


@dataclass
class Person(RedisBase):
    name: str
    age: int
    siblings = many_to_many(foreign_type="Person", backref="siblings")
    dad = many_to_one(foreign_type="Person", backref="children")
    children = one_to_many(foreign_type="Person", backref="dad")
    favourite_color = one_to_one("Color", backref="liker")


@dataclass
class Color(RedisBase):
    name: str
    liker = one_to_one(Person, backref="favourite_color")


red = Color.create(name="Red")
homer = Person.create(name="Homer", age=50, favourite_color=red)
print(f"homer.children={homer.children!r}")
print(f"homer.favourite_color={homer.favourite_color!r}")
bart = Person.create(name="Bart", age=11, dad=homer)
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
