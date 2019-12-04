import redis
import fakeredis
from typing import Optional, Callable
from redorm.settings import REDORM_URL
from redorm.exceptions import DevelopmentServerUsedInProd


class RedormClient:
    pool: redis.ConnectionPool
    client: redis.Redis
    get_key_indirect: Optional[Callable] = None
    get_set_indirect: Optional[Callable] = None

    def __init__(self):
        self.server = None
        self.bind(REDORM_URL)

    def bind(self, url) -> bool:
        if url:
            self.client = redis.Redis.from_url(url, decode_responses=True)
            self.register_scripts()
            return True
        else:
            # Lightweight fake redis
            self.client = fakeredis.FakeRedis(decode_responses=True)
            return False

    def register_scripts(self):
        self.get_key_indirect = self.client.register_script(
            "return {redis.call('get', redis.call('get', KEYS[1]))}"
        )
        self.get_set_indirect = self.client.register_script(
            """local l = {}
local keys = redis.call('smembers', KEYS[2])
for _,k in ipairs(keys) do
table.insert(l, redis.call('get', KEYS[1] .. k))
end
return l
"""
        )

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
