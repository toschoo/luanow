---------------------------------------------------------------------------
-- Lua NOWDB Stored Procedure IPC Library
---------------------------------------------------------------------------
 
--------------------------------------
-- (c) Tobias Schoofs, 2019
--------------------------------------
   
-- This file is part of the NOWDB Stored Procedure Support Library.

-- It provides in particular
--  - locking
--  - events
--  - queues

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

local ipc = {}

function ipc.createlock(lk)
  nowdb.execute_(string.format(
    [[create lock %s if not exists]], lk))
end

function ipc.droplock(lk)
  nowdb.execute_(string.format(
    [[drop lock %s if exists]], lk))
end

function ipc.lock(lk,mode,tmo)
  local modeclause = ''
  if not mode or mode == 'w' then
     modeclause = 'for writing'
  elseif mode == 'r' then
     modeclause = 'for reading'
  else
     error(string.format("unknown locking mode: '%s'", tostring(mode)))
  end
  local tmoclause = ''
  if tmo then
     tmoclause = string.format("set timeout = %d", tmo)
  end
  local rc, r = nowdb.pexecute(string.format(
    [[lock %s %s %s]], lk, modeclause, tmoclause))
  if rc ~= nowdb.OK then
     if rc == nowdb.TIMEOUT then
        return nowdb.TIMEOUT
     else
        nowdb.raise(rc, r)
     end
  end
  return nowdb.OK
end

function ipc.unlock(lk)
  nowdb.execute_(string.format(
    [[unlock %s]], lk))
end

function ipc.withlock(lk, mode, f, ...)
  local rc = ipc.lock(lk,mode)
  if rc ~= nowdb.OK then 
     nowdb.raise(rc, string.format("on locking %s", lk))
  end
  local ok, res = pcall(f, ...)
  ipc.unlock(lk)
  if not ok then error(res) end
  return res
end

function ipc.withxlock(lk, f, ...)
  return ipc.withlock(lk, 'w', f, ...)
end

return ipc
