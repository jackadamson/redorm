import re
import redis
import fakeredis
from redorm.settings import REDORM_URL
from redorm.exceptions import DevelopmentServerUsedInProd


class RedormClient:
    pool: redis.ConnectionPool
    client: redis.Redis

    def __init__(self):
        self.server = None
        self.bind(REDORM_URL)

    def bind(self, url) -> bool:
        if url:
            self.client = redis.Redis.from_url(url, decode_responses=True)
            return True
        else:
            # Lightweight fake redis
            self.client = fakeredis.FakeRedis(decode_responses=True)
            return False

    def init_app(self, app):
        url = app.config.get("REDORM_URL")
        if url:
            success = self.bind(url)
            if (
                not success
                and app.config.get("FLASK_ENV") == "production"
                and not app.config.get("REDORM_INBUILT")
            ):
                raise DevelopmentServerUsedInProd(
                    "Redorm was not provided a Redis URL and would fall back to the in-built Redis. "
                    "In the flask app config either set REDORM_URL to a valid redis connection string "
                    "or set REDORM_INBUILT to True (not recommended)"
                )


red = RedormClient()
