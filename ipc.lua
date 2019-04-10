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
end

function ipc.droplock(lk)
end

function ipc.lock(lk,mode)
  local m = not mode and 'w' or mode
  if m ~= 'r' and m ~= 'w' then error('unknown locking mode') end
  -- lock
end

function ipc.unlock(lk)
  -- unlock
end

function ipc.withlock(lk, mode, f, ...)
  ipc.lock(lk,mode)
  ok, res = pcall(f, ...)
  if not ok then
     ipc.unlock(lk)
     error(res)
  end
  ipc.unlock(lk)
  return res
end

function ipc.withxlock(lk, f, ...)
  return ipc.withlock(lk, 'w', f, ...)
end

return ipc
