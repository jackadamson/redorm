import warnings

import redis
import fakeredis
from typing import Optional
from redorm.settings import REDORM_URL

GET_SET_INDIRECT = """
local l = {}
local keys = redis.call('smembers', KEYS[2])
for _,k in ipairs(keys) do
    table.insert(l, redis.call('get', KEYS[1] .. k))
end
return l
"""

GET_KEY_INDIRECT = """
return {redis.call('get', redis.call('get', KEYS[1]))}
"""


class RedormClient:
    pool: redis.ConnectionPool
    client: redis.Redis
    get_set_indirect_sha: str
    get_key_indirect_sha: str

    def __init__(self, redorm_url=REDORM_URL):
        self.server = None
        self.bind(redorm_url)

    def bind(self, url):
        if url:
            self.client = redis.Redis.from_url(url, decode_responses=True)
        else:
            warnings.warn(
                "Redorm was not provided a Redis URL and will fall back to the in-built incredibly slow fake redis. "
                "Set REDORM_URL environment variable to fix.",
                RuntimeWarning,
            )
            # Fake redis for developement
            self.client = fakeredis.FakeRedis(decode_responses=True)
        self.get_key_indirect_sha = self.client.register_script(GET_KEY_INDIRECT).sha
        self.get_set_indirect_sha = self.client.register_script(GET_SET_INDIRECT).sha

    def init_app(self, app):
        url = app.config.get("REDORM_URL")
        if url:
            self.bind(url)


red = RedormClient()
