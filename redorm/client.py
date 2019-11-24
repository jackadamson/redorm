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
        match = re.match(
            r"redis://(?P<host>[^:^/]+)(?::(?P<port>[0-9]+))?(?:/(?P<db>[0-9]+))?", url,
        )
        if match:
            d = match.groupdict()
            host = d["host"]
            port = int(d["port"]) if d["port"] is not None else 6379
            db = int(d["db"]) if d["db"] is not None else 0
            self.pool = redis.ConnectionPool(host="localhost", port=6379, db=1)
            self.client = redis.Redis(connection_pool=self.pool)
            return True
        else:
            # Lightweight fake redis
            self.server = fakeredis.FakeServer()
            self.client = fakeredis.FakeStrictRedis(server=self.server)
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
