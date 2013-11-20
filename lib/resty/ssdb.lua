-- Copyright (C) 2013 LazyZhu (lazyzhu.com)
-- Copyright (C) 2013 IdeaWu (ideawu.com)
-- Copyright (C) 2012 Yichun Zhang (agentzh)


local sub = string.sub
local tcp = ngx.socket.tcp
local insert = table.insert
local concat = table.concat
local null = ngx.null
local pairs = pairs
local unpack = unpack
local setmetatable = setmetatable
local tonumber = tonumber
local tostring = tostring
local error = error
local gmatch = string.gmatch
local remove = table.remove

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 56)
_M._VERSION = '0.02'

local commands = {
    "set",                  "get",                 "del",
    "scan",                 "rscan",               "keys",
    "incr",                 "decr",                "exists",
    "multi_set",            "multi_get",           "multi_del",
    "multi_exists",
    "hset",                 "hget",                "hdel",
    "hscan",                "hrscan",              "hkeys",
    "hincr",                "hdecr",               "hexists",
    "hsize",                "hlist",
    --[[ "multi_hset", ]]   "multi_hget",          "multi_hdel",
    "multi_hexists",        "multi_hsize",
    "zset",                 "zget",                "zdel",
    "zscan",                "zrscan",              "zkeys",
    "zincr",                "zdecr",               "zexists",
    "zsize",                "zlist",
    --[[ "multi_zset", ]]   "multi_zget",          "multi_zdel",
    "multi_zexists",        "multi_zsize"

}

local mt = { __index = _M }

function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end


function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function _M.connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:connect(...)
end


function _M.set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function _M.get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


function _M.close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


local function _read_reply(sock)
    local vals = {}
    local nvals = 0
    while true do
        -- read block size
        local line, err, partial = sock:receive()
        if not line or #line==0 then
            -- packet end
            break
        end
        local d_len = tonumber(line)

        -- read block data
        local data, err, partial = sock:receive(d_len)
        nvals = nvals + 1
        insert(vals, data);

        -- ignore the trailing lf/crlf after block data
        local line, err, partial = sock:receive()
    end

    local v_num = #vals

    if v_num == 1 then
        return vals
    else
        remove(vals, 1)
        return vals
    end
end


local function _gen_req(args)
    local nargs = #args

    local req = new_tab(nargs + 1, 0)

    local nbits = 0
    for i = 1, nargs do
        local arg = args[i]

        if arg then
            if type(arg) ~= "string" then
                arg = tostring(arg)
            end

            nbits = nbits + 1
            req[nbits] = #arg .. "\n" .. arg .. "\n"
        else
            return nil, err
        end
    end
    nbits = nbits + 1
    req[nbits] = "\n"

    -- it is faster to do string concatenation on the Lua land
    return concat(req)
end


local function _do_cmd(self, ...)
    local args = {...}

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local req = _gen_req(args)

    local reqs = self._reqs
    if reqs then
        reqs[#reqs+1] = req
        return
    end

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    return _read_reply(sock)
end


for i = 1, #commands do
    local cmd = commands[i]

    _M[cmd] =
        function (self, ...)
            return _do_cmd(self, cmd, ...)
        end
end


function _M.multi_hset(self, hashname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        local i = 0
        for k, v in pairs(t) do
            array[i]   = k
            array[i+1] = v
            i = i + 2
        end
        -- print("key", hashname)
        return _do_cmd(self, "multi_hset", hashname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_hset", hashname, ...)
end


function _M.multi_zset(self, keyname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        local i = 0
        for k, v in pairs(t) do
            array[i]   = k
            array[i+1] = v
            i = i + 2
        end
        -- print("key", keyname)
        return _do_cmd(self, "multi_zset", keyname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_zset", keyname, ...)
end


function _M.init_pipeline(self)
    self._reqs = {}
end


function _M.cancel_pipeline(self)
    self._reqs = nil
end


function _M.commit_pipeline(self)
    local reqs = self._reqs
    if not reqs then
        return nil, "no pipeline"
    end

    self._reqs = nil

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send(reqs)
    if not bytes then
        return nil, err
    end

    local nvals = 0
    local nreqs = #reqs
    local vals = new_tab(nreqs, 0)
    for i = 1, nreqs do
        local res, err = _read_reply(sock)
        if res then
            nvals = nvals + 1
            vals[nvals] = res

        elseif res == nil then
            return nil, err

        else
            nvals = nvals + 1
            vals[nvals] = err
        end
    end

    return vals
end

function _M.array_to_hash(self, t)
    local h = {}
    for i = 1, #t, 2 do
        h[t[i]] = t[i + 1]
    end
    return h
end

function _M.add_commands(...)
    local cmds = {...}
    local newindex = class_mt.__newindex
    class_mt.__newindex = nil
    for i = 1, #cmds do
        local cmd = cmds[i]
        _M[cmd] =
            function (self, ...)
                return _do_cmd(self, cmd, ...)
            end
    end
    class_mt.__newindex = newindex
end

return _M
