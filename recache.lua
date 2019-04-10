---------------------------------------------------------------------------
-- Lua NOWDB Stored Procedure Result Caching Library
---------------------------------------------------------------------------
 
--------------------------------------
-- (c) Tobias Schoofs, 2019
--------------------------------------
   
-- This file is part of the NOWDB Stored Procedure Support Library.

-- It provides in particular result caches

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

local function getedgename(name)
  return "nowsup_recache_" .. name
end

local function getunidname(name)
  return "nowsup_recache_" .. name .. "_unid"
end

local function createrecacheid(nm)
  local stmt = [[create type nowsup_recacheid (
                     id uint primary key,
                     name text) if not exists]]
  print(stmt)
  nowdb.execute_(stmt)
  -- insert new cacheid with name 'name' (not nm)
end

local function createprocdesc(nm)
  local stmt = [[create type nowsup_procdesc (
                   procname text primary key,
                   npars uint) if not exists]]
  print(stmt)
  nowdb.execute_(stmt)
  stmt = [[create stamped edge nowsup_recacheid_proc (
             origin nowsup_recacheid,
             destin nowsup_procdesc,
             param  uint,
             ptype  uint,
             pvalue text) if not exists]]
  --[=[
  local ins = string.format([[insert into nowdb_nowsup_recacheid_proc (
                                   origin, destin, param, ptype, pvalue)
                               values (%d, %s]], rid, procd['name'])
  for i = 1, #procd['params'] do
      local stmt = ins
      stmt = stmt .. tostring(i) .. ', '
                  .. procd['params'][i]['type']  .. ', '
                  .. tostring(procd['params'][i]['value'] .. ')')
  end
  --]=]
  print(stmt)
  nowdb.execute_(stmt)
end

-- name, procname, parameter 1, 2, 3, ...
function recache.create(name, procd, pld)
  nowsupbase.create()
  local nm = getedgename(name)
  local myid = getunidname(name)

  unid.create(myid) 
  createrecacheid(nm)
  createprocdesc(nm)
  
  local stmt = string.format([[create stamped edge %s (
                                 origin nowsup_recacheid,
                                 destin nowsup_anyuint]], nm)
  for i = 1, #pld do
      local p = pld[i]
      stmt = stmt .. ',\n  ' .. p['name'] .. ' ' .. nowdb.nowtypename(p['type'])
  end
  stmt = stmt .. ')'
  print(stmt)
  nowdb.execute_(stmt)
end

function recache.drop(name)
  local nm = getedgename(name)
  local myid = getunidname(name)

  nowdb.execute_(string.format([[drop edge %s if exists]], nm))
  unid.drop(myid)
end

function recache.withcache(name, f, pars) 
  print("withcache: ")
  for i = 1, #pars do
     print(string.format("%d: %s", i, pars[i]))
  end
  -- search in procd
  -- if it exists:
  -- return a cursor on result (select according to describe)
  -- otherwise:
  -- execute f
  -- for each row in the result set:
  -- insert into result
  -- return cursor
end

return recache
