# RedORM

A simple Redis ORM that only a madman would use in production.  
The red in RedORM both means Redis as well as the color red, as red is the fastest colour!

## Quick Start

To install `pip install redorm`

```python
from dataclasses import dataclass
from redorm import RedormBase, one_to_one, one_to_many, many_to_one, many_to_many


@dataclass
class Person(RedormBase):
    name: str
    age: int
    siblings = many_to_many(foreign_type="Person", backref="siblings")
    dad = many_to_one(foreign_type="Person", backref="children")
    children = one_to_many(foreign_type="Person", backref="dad")
    favourite_color = one_to_one("Color", backref="liker")


@dataclass
class Color(RedormBase):
    name: str
    liker = one_to_one(Person, backref="favourite_color")
```
```python
>>> red = Color.create(name="Red")
>>> homer = Person.create(name="Homer", age=50, favourite_color=red)
>>> print(repr(homer.favourite_color))
Color(id='dcb9aa50-554a-40a5-9acf-7d86c982e5ee', name='Red')
>>> print(repr(homer.children))
[]
>>> bart = Person.create(name="Bart", age=11, dad=homer)
>>> print(repr(homer.children))
[Person(id='424cd574-5382-4d34-89da-7233b3928405', name='Bart', age=11)]
>>> print(repr(bart.favourite_color))
None
>>> blue = Color.create(name="Blue", liker=bart)
>>> print(repr(bart.favourite_color))
Color(id='dc9df3c2-c592-4d87-a45e-f88a346342b4', name='Blue')
>>> print(repr(blue.liker))
Person(id='424cd574-5382-4d34-89da-7233b3928405', name='Bart', age=11)
>>> lisa = Person.create(name="Lisa", age=9, dad=homer.id, siblings=[bart])
>>> print(repr(homer.children))
[Person(id='205a459a-572c-41af-bae3-e6e730aada97', name='Lisa', age=9), Person(id='424cd574-5382-4d34-89da-7233b3928405', name='Bart', age=11)]
>>> bart.dad = None
>>> print(repr(homer.children))
[Person(id='205a459a-572c-41af-bae3-e6e730aada97', name='Lisa', age=9)]
```

## Why Redorm?

- Thread Safe
- Very fast
- Super simple to use
- Very little boilerplate

# Why not Redorm?

- Made in an afternoon
- Unlikely to be maintained
- Not thoroughly tested
- NOT THOROUGHLY TESTED
- Writing your own ORM is fantastic for learning, but you should not use it in prod
