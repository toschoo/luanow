---------------------------------------------------------------------------
-- Lua NOWDB Stored Procedure Support Base Library
---------------------------------------------------------------------------
 
--------------------------------------
-- (c) Tobias Schoofs, 2019
--------------------------------------
   
-- This file is part of the NOWDB Stored Procedure Support Library.

-- It provides in particular basic types for the nowsup library

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
local nowsupbase = {}

function nowsupbase.create()
  nowdb.execute_([[create type nowsup_nirvana (
                     id uint primary key) if not exists
                 ]])
  nowdb.execute_([[create type nowsup_anyuint (
                     id uint primary key) if not exists
                 ]])
end

function nowsupbase.drop()
  nowdb.execute_([[drop type nowsup_nirvana if exists]])
  nowdb.execute_([[drop type nowsup_anyuint if exists]])
end

return nowsupbase
