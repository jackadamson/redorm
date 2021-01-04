import pytest
from redorm import red


@pytest.fixture
def clean_db():
    red.client.flushdb()
