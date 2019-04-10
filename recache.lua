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
                     id      uint primary key,
                     name    text,
                     created time) if not exists]]
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
             destin nowsup_nirvana,
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

local function paramsmatch(rid, created, pars)
  print("origin : " .. tostring(rid))
  print("created: " .. tostring(created))
  local stmt = string.format(
    [[select param, ptype, pvalue
        from nowsup_recacheid_proc
       where origin = %d
         and stamp  = %d]], rid, created)
  local candidates = {}
  print(stmt)
  local cur = nowdb.execute(stmt)
  for row in cur.rows() do
      local i = row.field(0)
      if tostring(pars[i]) == row.field(2) then
         if candidates[rid] == 0 then candidates[rid] = 1 end
      elseif candidates[rid] == 1 then
         candiates[rid] = 2 
      end
  end
  cur.release()
  for k, v in ipairs(candidates) do
      if v == 1 then return k end
  end
end

local function findcache(name, valid, f, pars)
  local stmt = string.format(
    [[select id, created from nowsup_recacheid
       where name = '%s']], name)
  print(stmt)
  local cur = nowdb.execute(stmt)
  for row in cur.rows() do
     local rid = row.field(0)
     print("RID: " .. tostring(rid))
     if valid(rid) then
        if paramsmatch(rid, row.field(1), pars) then
           cur.release()
           return rid
        end
     end
  end
end

local function cachecursor(nm, rid)
  local cur = nowdb.execute("describe " .. nm)
  local fs = ''
  local first = true
  for row in cur.rows() do
      local f = row.field(0)
      print("describe " .. nm .. ": " .. f)
      if f ~= 'origin' then
         if not first then fs = fs .. ', ' else first = false end
         fs = fs .. f
      end
  end
  cur.release()
  local stmt = string.format(
    [[select %s from %s
       where origin = %d]], fs, nm, rid)
  print(stmt)
  return nowdb.execute(stmt)
end

local function insertrid(name)
  local myid = getunidname(name)
  local rid = unid.get(myid)
  local now = nowdb.getnow()
  nowdb.execute_(string.format(
    [[insert into nowsup_recacheid (id, name, created)
      values (%d, '%s', %d)]], rid, name, now))
  return rid, now
end

local function insertparams(rid, now, pars)
  local ins =
    [[insert into nowsup_recacheid_proc(
        origin, destin, stamp, param, pvalue)
      values (%d, 1, %d, %d, '%s')]]
  for i = 1, #pars do
     nowdb.execute_(string.format(ins, 
      rid, now, i, tostring(pars[i])))
  end
end

local function describeme(name)
  local cur = nowdb.execute("describe " .. name)
  local edge = {}
  local i = 0
  for row in cur.rows() do
      i = i + 1
      print("adding " .. i .. ": " .. row.field(0))
      edge[i] = row.field(0)
  end
  cur.release()
  return edge
end

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

local function generatecache(name, rid, now, co)
  local nm = getedgename(name)
  local edge = describeme(nm)
  local ins = [[insert into %s (]] 
  print("edge: " .. #edge)
  for i = 1, #edge do
    if i > 1 then ins = ins .. ', ' end
    ins = ins .. edge[i]
  end
  ins = ins .. ') values (%d, 1, %d %s'
  while true do
     if coroutine.status(co) == 'dead' then break end
     local _, row = coroutine.resume(co)
     if not row then break end
     local vals = getstmt(row)
     vals = vals .. ')'
     local stmt = string.format(ins, nm, rid, now, vals)
     print(stmt)
     nowdb.execute_(stmt)
  end
  return cachecursor(nm, rid)
end

local function fillcache(name, co, pars)
  local rid, now = insertrid(name)
  insertparams(rid, now, pars)
  return generatecache(name, rid, now, co)
end

function recache.withcache(name, valid, co, pars) 
  local nm = getedgename(name)
  local rid = findcache(name, valid, co, pars)
  if rid then 
     return cachecursor(nm, rid)
  end
  return fillcache(name, co, pars)
end

return recache
