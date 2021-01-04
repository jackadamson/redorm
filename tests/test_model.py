from dataclasses import dataclass, field
import pytest
from redorm import RedormBase
from redorm.exceptions import MultipleInstancesReturned, UniqueContstraintViolation


# All data from https://simpsons.fandom.com/wiki or madeup


@dataclass
class User(RedormBase):
    username: str = field(metadata={"unique": True})
    favourite_colour: str = field(metadata={"index": True})
    job: str = field(metadata={"index": True})
    phrase: str


@pytest.fixture
def homer():
    return User.create(username="homer", favourite_colour="red", phrase="Doh", job="Safety Inspector")


@pytest.fixture
def bart():
    return User.create(username="bart", favourite_colour="red", phrase="Ay Caramba", job="Student")


@pytest.fixture
def marge():
    return User.create(username="marge", favourite_colour="green", phrase="Hmmmm", job="Housewife")


def test_create_instance(clean_db):
    homer = User.create(username="homer", favourite_colour="red", phrase="Doh", job="Safety Inspector")
    assert homer.username == "homer"


def test_get_by_id(clean_db, homer):
    user_id = homer.id
    user = User.get(user_id)
    assert user.id == homer.id
    assert user.username == homer.username
    assert user.favourite_colour == homer.favourite_colour


def test_get_by_unique(clean_db, homer, bart):
    user = User.get(username="homer")
    assert homer.id == user.id


def test_get_by_index(clean_db, homer, marge):
    user = User.get(favourite_colour="red")
    assert homer.id == user.id


def test_get_multiple_results(clean_db, homer, bart):
    with pytest.raises(MultipleInstancesReturned):
        user = User.get(favourite_colour="red")


def test_get_intersection(clean_db, homer, bart):
    red_liking_student = User.get(favourite_colour="red", job="Student")
    assert red_liking_student.id == bart.id


def test_list(clean_db, homer):
    users = User.list()
    assert len(users) == 1
    assert homer in users

    bart = User.create(username="bart", favourite_colour="red", phrase="Ay Caramba", job="Student")
    users = User.list()
    assert len(users) == 2
    assert homer in users
    assert bart in users


def test_list_by_index(clean_db, homer, bart):
    red_favourites = User.list(favourite_colour="red")
    assert len(red_favourites) == 2


def test_unique(clean_db, bart):
    with pytest.raises(UniqueContstraintViolation):
        other_bart = User.create(username="bart", favourite_colour="red", phrase="Ay Caramba", job="Student")
    user = User.get(username="bart")
    assert user.id == bart.id
