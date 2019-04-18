---------------------------------------------------------------------------
-- Lua NOWDB Stored Procedure Result Caching
---------------------------------------------------------------------------
 
--------------------------------------
-- (c) Tobias Schoofs, 2019
--------------------------------------
   
-- This file is part of the NOWDB Stored Procedure Support Library.

-- It provides in particular result caching

-- The NOWDB Stored Procedure Support Library
-- is free software; you can redistribute it
-- and/or modify it under the terms of the GNU Lesser General Public
-- License as published by the Free Software Foundation; either
-- version 2.1 of the License, or (at your option) any later version.
  
-- The NOWDB Stored Procedure Support Library
-- is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
-- Lesser General Public License for more details.
  
-- You should have received a copy of the GNU Lesser General Public
-- License along with the NOWDB CLIENT Library; if not, see
-- <http://www.gnu.org/licenses/>.
---------------------------------------------------------------------------
local nowsupbase = require('nowsupbase')
local unid = require('unid')

local recache = {}

---------------------------------------------------------------------------
-- Result Cache consists in
-- ------------
-- a type that stores cacheids, which is named
--    RECACHETYPE
-- the primary key is a unique identifier named
--    SEQ
-- we further have the edge table
--    RECACHETYPE_proc,
-- which stores the parameter values with which
-- a given resultset was created
--
-- Per cache an edge table named nowsup_recache_<name>
-- (where <name> is the name of the result cache)
-- is created. This table stores the effective results
---------------------------------------------------------------------------
local RECACHETYPE = 'nowsup_recacheid'
local SEQ = 'recache'

-- debug: print all statements
local DBG = false

---------------------------------------------------------------------------
-- turn debug on/off
---------------------------------------------------------------------------
function recache.setDebug(t) DBG = t end

-- Test whether the edge table for our cache exists
local function edgeexists(nm)
  rc, cur = nowdb.pexecute('describe ' .. nm)
  if rc == nowdb.OK then
     cur.release()
     return true
  end
  if rc == nowdb.KEYNOF then return false end
  nowdb.raise(rc, cur)
end

-- convert cache name to edge table name
local function getedgename(name)
  return "nowsup_recache_" .. name
end

-- create cacheid table (if not exists)
local function createrecacheid(nm)
  local stmt = string.format(
    [[create type %s (
        id      uint primary key,
        name    text,
        created time) if not exists]], RECACHETYPE)
  if DBG then print(stmt) end
  nowdb.execute_(stmt)
end

-- create proc desc table (if not exists)
local function createprocdesc(nm)
  stmt = string.format(
     [[create stamped edge %s_proc (
         origin %s,
         destin nowsup_nirvana,
         param  uint,
         ptype  uint,
         pvalue text) if not exists]],
     RECACHETYPE, RECACHETYPE)
  if DBG then print(stmt) end
  nowdb.execute_(stmt)
end

---------------------------------------------------------------------------
-- Create a new result cache
-- -------------------------
-- name is the name of the new result cache,
-- pld  is the payload descriptor, i.e.
--      it describes the rows the result consists of
--      the pld is an array of tables each of which
--      has two keys: 'name' and 'type' describing
--      the fields of the payload.
---------------------------------------------------------------------------
function recache.create(name, pld)
  local nm = getedgename(name)

  -- if this table exists, we are done
  -- (or the whole thing is inconsistent)
  if edgeexists(nm) then return end

  -- create base stuff
  nowsupbase.create()

  print("CREATING CACHE")
  unid.create(SEQ) 
  createrecacheid(nm)
  createprocdesc(nm)
  
  -- create the edge table according to
  -- the payload descriptor (pld)
  local stmt = string.format(
    [[create stamped edge %s (
        origin %s,
        destin nowsup_anyuint]], nm, RECACHETYPE)
  for i = 1, #pld do
      local p = pld[i]
      stmt = stmt .. ',\n  ' .. p['name'] .. ' ' ..
              nowdb.nowtypename(p['type'])
  end
  stmt = stmt .. ')'
  if DBG then print(stmt) end
  nowdb.execute_(stmt)
end

---------------------------------------------------------------------------
-- Drop result cache
---------------------------------------------------------------------------
function recache.drop(name)
  local nm = getedgename(name)
  nowdb.execute_(string.format([[drop edge %s if exists]], nm))
end

-- check if the parameters match
local function paramsmatch(rid, created, pars)
  if not pars then return true end
  local stmt = string.format(
    [[select param, pvalue
        from %s_proc
       where origin = %d
         and stamp  = %d]], RECACHETYPE, rid, created)
  local candidate = 0
  if DBG then print(stmt) end
  local cur = nowdb.execute(stmt)
  for row in cur.rows() do
      local i = row.field(0)
      local v = row.field(1)
      -- consider using ptype
      if (not v and not pars[i]) or
         (tostring(pars[i]) == row.field(1))
      then
         if candidate == 0 then candidate = 1 end
      else
         candiate = 0
         break
      end
  end
  cur.release()
  return (candidate == 1)
end

-- find valid resultcache with matching parameters
local function findcache(name, valid, pars)
  local stmt = string.format(
    [[select id, created from %s 
       where name = '%s']], RECACHETYPE, name)
  local _rid, _now = 0, 0
  if DBG then print(stmt) end
  local cur = nowdb.execute(stmt)
  for row in cur.rows() do
     local rid = row.field(0)
     local now = row.field(1)
     if valid(rid) then
        if paramsmatch(rid, now, pars) then
           if _now < now then -- select the latest
              _rid, _now = rid, now 
           end
        end
     else
       -- delete this entry
     end
  end
  cur.release()
  if _rid == 0 then return nil else return _rid end
end

-- create cursor on cached result
local function cachecursor(nm, rid)
  local cur = nowdb.execute("describe " .. nm)
  local fs = ''
  local first = true
  for row in cur.rows() do
      local f = row.field(0)
      if f ~= 'origin' and f ~= 'destin' and f ~= 'stamp' then
         if not first then fs = fs .. ', ' else first = false end
         fs = fs .. f
      end
  end
  cur.release()

  local stmt = string.format(
    [[select %s from %s
       where origin = %d]], fs, nm, rid)
  if DBG then print(stmt) end
  return nowdb.execute(stmt)
end

-- insert recache id
local function insertrid(name)
  local rid = unid.get(SEQ)
  local now = nowdb.getnow()
  nowdb.execute_(string.format(
    [[insert into %s (id, name, created)
      values (%d, '%s', %d)]], RECACHETYPE, rid, name, now))
  return rid, now
end

-- insert parameter setting
local function insertparams(rid, now, pars)
  if not pars then return nil end
  local ins =
    [[insert into %s_proc(
        origin, destin, stamp, param, pvalue)
      values (%d, 1, %d, %d, '%s')]]
  for i = 1, #pars do -- we could use ipairs here
     stmt = string.format(ins, RECACHETYPE,
            rid, now, i, tostring(pars[i]))
     if DBG then print(stmt) end
     nowdb.execute_(stmt)
  end
end

-- returns an array containing the fields of
-- the recache edge table
local function describeme(nm)
  local cur = nowdb.execute("describe " .. nm)
  local edge = {}
  local i = 0
  for row in cur.rows() do
      i = i + 1
      edge[i] = row.field(0)
  end
  cur.release()
  return edge
end

-- creates the value part of the statement
-- to insert the results into the cache table
local function getstmt(row)
  local stmt = ''
  for i = 1, row.countfields() do
      local t, v = row.typedfield(i-1)
      if t == nowdb.TEXT then
         stmt = stmt .. ", '" .. v .. "'"
      elseif t == nowdb.NOTHING then
         stmt = stmt .. ", NULL"
      else
         stmt = stmt .. ", " .. tostring(v)
      end
  end
  return stmt
end

-- generate the cache results using the coroutine
-- and insert them into the cache table
local function generatecache(name, rid, now, co)
  local nm = getedgename(name)
  local edge = describeme(nm)
  -- fixed part of the insert statement
  local ins = [[insert into %s (]] 
  for i = 1, #edge do -- we could use ipairs here
    if i > 1 then ins = ins .. ', ' end
    ins = ins .. edge[i]
  end

  -- the final '%s' is the variable part (the values)
  ins = ins .. ') values (%d, 1, %d %s'

  -- iterate over the coroutine
  for row in nowdb.corows(co) do
     local vals = getstmt(row) -- get the values 
     vals = vals .. ')'
     local stmt = string.format(ins, nm, rid, now, vals)
     if DBG then print(stmt) end
     nowdb.execute_(stmt)
  end
  return cachecursor(nm, rid)
end

-- create a new cache line with parameter settings
-- and generate the corresponding results
local function fillcache(name, co, pars)
  local rid, now = insertrid(name)
  insertparams(rid, now, pars)
  return generatecache(name, rid, now, co)
end

---------------------------------------------------------------------------
-- Use either existing and valid cache results
-- or create a new cache entry consisting of
-- the results produced by co
-- the function returns a cursor on the result set
-- the parameters are:

-- + the cache name

-- + a coroutine that, per resume, produces one row
--   to insert into the result cache; this row must comply to the
--   payload descriptor provided to nowdb.create.

-- + a function to evaluate if the the particular cache entry is valid.
--   This function needs to accept the identifier of the cache entry.
--   Some standard validators are provided below.

-- + an array containing the parameter values with which
--   the stored procedure was called, for instance,
--   if the procedure is of the form:
--   myproc(name text, lat float, lon float),
--   a valid parameter setting may be:
--   {'lisbon', 38.0, -9.0}.
--   If this array is nil, the cache entry
--   will be valid for any combination of parameters.
---------------------------------------------------------------------------
function recache.withcache(name, co, valid, pars) 
  local nm = getedgename(name)
  if not edgeexists(nm) then -- must exist
     nowdb.raise(nowdb.KEYNOF, 'result cache does not exist')
  end 
  local rid = findcache(name, valid, pars)
  if rid then return cachecursor(nm, rid) end
  return fillcache(name, co, pars)
end

---------------------------------------------------------------------------
-- Equivalent to withcache(name, co, recache.valid),
-- i.e. a cache that never expires and
-- valid for all combinations of parameters
---------------------------------------------------------------------------
function recache.staticresult(name, co)
  return recache.withcache(name, co, recache.valid)
end

---------------------------------------------------------------------------
-- Equivalent to withcache(name, co, recache.invalid),
-- i.e. a cache that expires immediately
---------------------------------------------------------------------------
function recache.tempresult(name, co)
  return recache.withcache(name, co, recache.invalid)
end

---------------------------------------------------------------------------
-- Validator: always valid (no expiration)
---------------------------------------------------------------------------
function recache.valid()
  return true
end

---------------------------------------------------------------------------
-- Validator: never valid (expires immediately)
---------------------------------------------------------------------------
function recache.invalid()
  return false
end

-- Validator: expires after period * unit 
local function expires(rid, period, unit)
  local created = nowdb.onevalue(string.format(
    [[select created from %s where id = %d]], RECACHETYPE, rid))
  local now = nowdb.getnow()
  local xpr = created + period * unit
  return (xpr > now)
end

---------------------------------------------------------------------------
-- Returns a validator for caches that expire in several days
---------------------------------------------------------------------------
function recache.expiresindays(d)
   return function(rid)
      return expires(rid, d, nowdb.day)
   end
end

---------------------------------------------------------------------------
-- Returns a validator for caches that expire in several hour 
---------------------------------------------------------------------------
function recache.expiresinhours(d)
   return function(rid)
      return expires(rid, d, nowdb.hour)
   end
end

---------------------------------------------------------------------------
-- Returns a validator for caches that expire in several minutes
---------------------------------------------------------------------------
function recache.expiresinminutes(d)
   return function(rid)
      return expires(rid, d, nowdb.minute)
   end
end

---------------------------------------------------------------------------
-- Returns a validator for caches that expire in several seconds
---------------------------------------------------------------------------
function recache.expiresinseconds(d)
   return function(rid)
      return expires(rid, d, nowdb.second)
   end
end

---------------------------------------------------------------------------
-- Return the package
---------------------------------------------------------------------------
return recache
