--
-- Created by IntelliJ IDEA.
-- User: jack
-- Date: 4/1/21
-- Time: 3:23 pm
-- To change this template use File | Settings | File Templates.
--
local l = {}
local keys = redis.call('smembers', KEYS[2])
for _,k in ipairs(keys) do
    table.insert(l, redis.call('get', KEYS[1] .. k))
end
return l
