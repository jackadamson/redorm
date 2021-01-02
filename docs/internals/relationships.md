# Relationships

Annotations:  
- All caps bold indicates a redis command, eg. **SET**
- Lower case italics are redis data types, eg. *string*

## Entities

The below example 
```python
@dataclass
class Person(RedormBase):
    name: str = field(metadata={"unique": True})
    siblings = many_to_many(foreign_type="Person", backref="siblings")
    dad = many_to_one(foreign_type="Person", backref="children")
    children = one_to_many(foreign_type="Person", backref="dad")
    catch_phrase = one_to_one("CatchPhrase", backref="person")


@dataclass
class CatchPhrase(RedormBase):
    phrase: str = field(metadata={"index": True})
    person = one_to_one(Person, backref="catch_phrase")

homer = Person.create(name="Homer", age=50)
bart = Person.create(name="Bart", age=11, dad=homer)
phrase = CatchPhrase(phrase="Ay, Caramba")
bart.catch_phrase = phrase
```

## Relationships

TODO
