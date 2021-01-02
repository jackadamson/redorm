# Internals

Annotations:  
- All caps bold indicates a redis command, eg. **SET**
- Lower case italics are redis data types, eg. *string*

## Entities

Models are stored as JSON in redis. All models implicitly have an `id` field which is an auto generated UUID.  
For example, the following
```python
@dataclass
class Role(RedormBase):
    name: str = field(metadata={"unique": True})
    level: int = field(metadata={"index": True})
    description: str
# Note: You don't need to supply an ID, it is just for illustration.
admin = Role.create(id="858b1d16-04a4-421f-a515-816b725ac186", name="admin", description="Can do anything", level=100)
```

Would **SET** the *string* `Role:member:858b1d16-04a4-421f-a515-816b725ac186` to 
```
{"description": "guest", "id": "08a4bfe8-9905-4d83-a05a-5019f434c55", "name": "guest", "level": 100}
```

The UUID is **SADD** to a *set* called `Role:all` which is used for listing all instances of an model.

## Unique Constraint

Uniqueness in implemented with a Redis Hash Map.

Using the above example, there would be a *hash* located at `Role:key:name` containing 

```
{"admin": "858b1d16-04a4-421f-a515-816b725ac186"}
```

## Indexes

To search for instances with a value for a given attribute, an index is created implemented with a Redis *set*.  

Stored at the location `Model:index:attribute:value` as a set of IDs that have the given value for the attribute.

In the above example, it would set `Role:index:level:100` to the *set* `{'858b1d16-04a4-421f-a515-816b725ac186'}`.

## Summary So Far

| Functionality | Pattern | Redis Type |
| ------------- | ------- | ---------- |
| Unique Constraint | `ModelName:key:attribute` | *hash* |
| Index | `ModelName:key:attribute:value` | *set* |
| Instances IDs | `ModelName:all` | *set* |
| Instance Contents | `ModelName:member:id` | *string* |
