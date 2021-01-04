import pytest
from redorm import red


@pytest.fixture
def clean_db():
    red.bind("redis://localhost")
    red.client.flushdb(asynchronous=False)
    red.setup_scripts()
