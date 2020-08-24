local math = require("math")
local math_floor = math.floor
local ngx_now = ngx.now
local setmetatable = setmetatable

local _M = {
    _VERSION = '0.01'
}

local mt = {
    __index = _M
}

function _M.new(reqs, window, cache, expire)
    if not expire then
        expire = window * 1.5
    else
        expire = expire
    end
    local self = {
        reqs = reqs,
        window = window * 1000,
        expire = expire,
        cache=cache
    }
    return setmetatable(self, mt)
end

function _M.incoming(self, key, commit)
    local window = self.window
    local now = ngx_now() * 1000
    local cache = self.cache
    -- current window key suffix
    local cs = math_floor(now / window) 
    -- last window key suffix
    local ps = cs - 1

    local ckey = key .. ':' .. cs
    local pkey = key .. ':' .. ps

    -- reqs in last window
    local lq, err = cache:get(pkey)
    if not lq then
        -- return nil, err
        lq = 0
    end

    -- reqs in this window
    local cq, err = cache:get(ckey)
    if not cq then
        -- return nil, err
        cq = 0
    end


    -- valid reqs in last window
    local lv = (1 - ((now % window)) / window) * lq

    if self.reqs > (lv + cq) then
        if commit then
            local n, err = cache:incr(ckey, 1, 0)
            cache:expire(ckey, self.expire)
            if not n then
                return nil, err
            end
        end
        return true, ''
    else
        return false, ''
    end
end

return _M
