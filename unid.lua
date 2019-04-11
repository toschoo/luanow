---------------------------------------------------------------------------
-- Lua NOWDB Stored Procedure Unique Identifier Library
---------------------------------------------------------------------------
 
--------------------------------------
-- (c) Tobias Schoofs, 2019
--------------------------------------
   
-- This file is part of the NOWDB Stored Procedure Support Library.

-- It provides in particular services for unique identifiers

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
local ipc = require('ipc')
local unid = {}

local function getedgename(name)
  return "nowsup_unid_" .. name
end

local function getlockname(name)
  return string.format([[nowsup_unid_%s_lk]], name)
end

local function unidexists(nm)
  local rc, cur = nowdb.pexecute('describe ' .. nm)
  if rc == nowdb.OK then
     cur.release()
     return true
  end
  if rc == nowdb.KEYNOF then return false end
  nowdb.raise(rc, cur)
end

function unid.create(name)
  local nm = getedgename(name)
  local lk = getlockname(name)

  if unidexists(nm) then return end

  nowsupbase.create()
  
  ipc.createlock(lk)
  nowdb.execute_([[create type nowsup_uniqueid (
                     id uint primary key,
                     name text) if not exists
                 ]])
  nowdb.execute_(string.format([[create stamped edge %s (
                                   origin nowsup_uniqueid,
                                   destin nowsup_nirvana,
                                   comment text
                                 ) if not exists]], nm))
  math.randomseed(os.time())
  local x = math.random(2^8, 2^31)
  nowdb.execute_(string.format([[insert into nowsup_uniqueid (id, name)
                                        values  (%d, '%s')]], x, nm))
  nowdb.execute_(string.format([[insert into %s (origin, destin, stamp)
                                        values  (%d, 1, now())]], nm, x))
end

function unid.drop(name)
  local nm = getedgename(name)
  local lk = getlockname(name)
  nowdb.execute_(string.format([[drop edge %s if exists]], nm))
  ipc.droplock(lk)
end

local function uget(name)
  local nm = getedgename(name)
  local sql = string.format([[select max(origin) from %s]], nm)
  local x = nowdb.onevalue(sql) + 1
  nowdb.execute_(string.format([[insert into %s (origin, destin, stamp)
                                        values  (%d, 1, now())]], nm, x))
  return x
end

function unid.get(name)
  local lk = getlockname(name)
  return ipc.withxlock(lk, uget, name)
end

return unid
