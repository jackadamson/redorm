-- Saves a model, provided it satisfies unique constraints otherwise doesn't side effect
--

local uniquecnt = ARGV[1]
local uniquenullcnt = ARGV[2]
local indexcnt = ARGV[3]
local indexnullcnt = ARGV[4]
local data = ARGV[5]
local uuid = ARGV[6]
local clsname = ARGV[7]

local beginunique = 8
local endofunique = beginunique+(uniquecnt*3)-1
-- Check uniqueness constraints, triples of field name, old value, new value
for i=beginunique,endofunique,3 do
     if redis.call('hexists', clsname .. ':key:' .. ARGV[i], ARGV[i+2]) == 1 then
         return redis.error_reply('Unique Violation: ' .. ARGV[i])
     end
end

-- Update unique hash maps
for i=beginunique,endofunique,3 do
    redis.call('hdel', clsname .. ':key:' .. ARGV[i], ARGV[i+1])
    redis.call('hset', clsname .. ':key:' .. ARGV[i], ARGV[i+2], uuid)
    redis.call('srem', clsname .. ':keynull:' .. ARGV[i], uuid)
end

local beginuniquenull = endofunique + 1
local endofuniquenull = beginuniquenull+(uniquenullcnt*2)-1
-- Update unique hash maps, pairs of field name, old value
for i=beginuniquenull,endofuniquenull,2 do
    redis.call('hdel', clsname .. ':key:' .. ARGV[i], ARGV[i+1])
    redis.call('sadd', clsname .. ':keynull:' .. ARGV[i], uuid)
end

local beginindex = endofuniquenull + 1
local endofindex = beginindex+(indexcnt*3)-1
-- Update index sets, triples of field name, old value, new value
for i=beginindex,endofindex,3 do
    -- Remove from old index
    redis.call('srem', clsname .. ':index:' .. ARGV[i] .. ':' .. ARGV[i+1], uuid)
    -- Remove from old null index in case was null
    redis.call('srem', clsname .. ':indexnull:' .. ARGV[i], uuid)
    -- Add to new index
    redis.call('sadd', clsname .. ':index:' .. ARGV[i] .. ':' .. ARGV[i+2], uuid)
end

local beginindexnull = endofindex + 1
local endofindexnull = beginindexnull+(indexnullcnt*2)-1
-- Update unique hash maps, pairs of field name, old value
for i=beginindexnull,endofindexnull,2 do
    -- Remove from old index
    redis.call('srem', clsname .. ':index:' .. ARGV[i] .. ':' .. ARGV[i+1], uuid)
    -- Add to null index
    redis.call('sadd', clsname .. ':indexnull:' .. ARGV[i], uuid)
end
redis.call('set', clsname .. ':member:' .. uuid, data)
redis.call('sadd', clsname .. ':all', uuid)
