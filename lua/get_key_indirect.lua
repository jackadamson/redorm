--
-- Created by IntelliJ IDEA.
-- User: jack
-- Date: 4/1/21
-- Time: 3:24 pm
-- To change this template use File | Settings | File Templates.
--

return {redis.call('get', redis.call('get', KEYS[1]))}
