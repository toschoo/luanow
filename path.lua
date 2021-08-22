---------------------------------------------------------------------------
-- Lua NOWDB Stored Procedure path finding and related functions
---------------------------------------------------------------------------
 
--------------------------------------
-- (c) Tobias Schoofs, 2021
--------------------------------------
   
-- This file is part of the NOWDB Stored Procedure Support Library.

-- It provides in particular path finding and related services.

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
local path = {}

---------------------------------------------------------------------------
-- Chunksize for creating SQL statements with large in list
------------
-- e.g.: select * from link where origin in (...)
---------------------------------------------------------------------------
path.CHUNKSZ = 100000

-- Split a list into chunks of chunksz
local function chunker(list, chunksz)
  return function(list,i)
    if i > #list then return nil end
    z = i + chunksz
    if z > #list then
       return z, {table.unpack(list, i, #list)}
    else
       return z, {table.unpack(list, i, z-1)}
    end
  end, list, 1
end

-- Convert a string representing the list of an 'in' condition
local function toinstr(keys)
  s = "("
  for i, k in pairs(keys) do
    if i > 1 then s = s .. ", " end
    s = s .. k
  end
  return s .. ")"
end

---------------------------------------------------------------------------
-- Generate an SQL statement that selects the next or previous
-- generation of nodes
-- keys   : list of numeric keys to be converted to an 'in' condition
-- edge   : the edge from which to select
-- forward: boolean distinguishing forward and backward
---------------------------------------------------------------------------
local function nextgen(keys, edge, forward)
  if forward then
    return string.format(
      [[select * from %s
         where origin in %s]],
      edge, toinstr(keys))
  else
    return string.format(
      [[select * from %s
         where destin in %s]],
      edge, toinstr(keys))
  end
end

---------------------------------------------------------------------------
-- Search forward
-- --------------
-- target  : the target node
-- arc     : the edge we are operating on
-- nextGen : list of keys for the generation we are searching
-- prevGen : dictionary key -> keys, the pervious generations
--           (we don't want to search them)
-- forward : list of keys searched forward, we add our results to this list
-- backward: list of keys to check if we have found a link
---------------------------------------------------------------------------
local function fsearch(target, arc, nextGen, prevGen, forward, backward)
   local tmp = {}
   for _, keys in chunker(nextGen, path.CHUNKSZ) do
     local stmt = nextgen(keys, arc, true)
     -- print(stmt)
     local cur = nowdb.execute(stmt)
     for row in cur.rows() do
       -- print(row.field(0) .. " -> " .. row.field(1))
       local k = row.field(0)
       local n = row.field(1)
       if forward[n] then
          forward[n][#forward[n]+1] = k
       else
          forward[n] = {k}
       end
       if n == target then return n end
       if not prevGen[n] then tmp[#tmp+1] = n end
     end
     cur.release()
     for _, k in pairs(nextGen) do
         prevGen[k] = true
     end
     for k, _ in pairs(forward) do
       if backward[k] then return k end
     end
   end
   return -1, tmp
end

---------------------------------------------------------------------------
-- Search backward
-- ---------------
-- The code is almost identical to fsearch.
-- The two functions could be merged!
---------------------------------------------------------------------------
local function bsearch(target, arc, nextGen, prevGen, forward, backward)
   local tmp = {}
   for _, keys in chunker(nextGen, path.CHUNKSZ) do
     local stmt = nextgen(keys, arc, false)
     -- print(stmt)
     local cur = nowdb.execute(stmt)
     for row in cur.rows() do
       -- print(row.field(0) .. " -> " .. row.field(1))
       local k = row.field(0)
       local n = row.field(1)
       if backward[k] then
          backward[k][#backward[k]+1] = n
       else
          backward[k] = {n}
       end
       if k == target then return k end
       if not prevGen[k] then tmp[#tmp+1] = k end
     end
     cur.release()
     for _, k in pairs(nextGen) do
         prevGen[k] = true
     end
     for k, _ in pairs(backward) do
       if forward[k] then return k end
     end
   end
   return -1, tmp
end

---------------------------------------------------------------------------
-- Database path finding
-- ---------------------
-- root  : The node we are starting with
-- target: The node to which we search a path
-- arc   : The edge we are following
-- it    : The number of iterations
-- We search in two directions, from root forward and from target backward.
-- We stop when we found a node that appears in both directions or when
-- the number of iterations has been reached. In this case we give up.
---------------------------------------------------------------------------
local function shortestInDB(root, target, arc, it)
  local forward  = {}
  local backward = {}
  local fnextgen = {root}
  local bnextgen = {target}
  local fprevgen = {}
  local bprevgen = {}
  local link = -1

  for i = 1, it do
      -- print("iteration " .. i)
      link, fnextgen = fsearch(target, arc, fnextgen, fprevgen, forward, backward)
      if link ~= -1 then break end

      link, bnextgen = bsearch(root, arc, bnextgen, bprevgen, forward, backward)
      if link ~= -1 then break end
  end
  return link, forward, backward
end 

---------------------------------------------------------------------------
-- Find the path to 'root' starting with the list of neighbours.
-- -----------------------
-- root      : the node we are searching
-- neighbours: The list of neighbours we are starting with
-- graph     : dictionary keys -> neightbours
-- seen      : dictiornary of nodes we already followed
-- step      : current number of recursions (max 25)
---------------------------------------------------------------------------
local function findPath(root, neighbours, graph, p, seen, step)
  if step > 25 then return {} end
  for _, k in pairs(neighbours) do
      if not seen[k] then
         seen[k] = true 
         if k == root then return true end
         p[#p+1] = k
         rc = findPath(root, graph[k], graph, p, seen, step+1)
         if rc then return rc end
	 p[#p] = nil
      end
  end
  return false
end

---------------------------------------------------------------------------
-- Link two paths from which we know they are linked through node 'link'
-----------------
-- root    : starting node
-- target  : end node
-- forward : dictionary key -> neighbours from forward search
-- backward: dictionary key -> neighbours from backward search
---------------------------------------------------------------------------
local function linkPaths(root, target, link, forward, backward)
  if root == target then return {root} end
  local p1 = {}
  local p2 = {}
  if forward and forward[link] then
     findPath(root  , forward[link] , forward , p1, {}, 0)
  end
  if backward and backward[link] then
     findPath(target, backward[link], backward, p2, {}, 0)
  end
  return p1, p2
end

---------------------------------------------------------------------------
-- Join two paths at link starting with root ending with target
-- --------------
-- root  : the starting node
-- p1    : first half of the path (in reversed order)
-- link  : the link between the two paths
-- p2    : second half of the path (in correct order)
-- target: the end node
---------------------------------------------------------------------------
local function joinPaths(root, p1, link, p2, target)
  local r = {root}
  for i = #p1, 1, -1 do -- reverse!
      r[#r+1] = p1[i]
  end
  if link ~= root and link ~= -1 then
     r[#r+1] = link
  end
  for _, v in pairs(p2) do
      r[#r+1] = v
  end
  if link ~= target then
     r[#r+1] = target
  end
  return r
end

-- Convert a path to string (debugging) 
local function showPath(mypath)
  local p = ""
  for i, v in pairs(mypath) do
      if i > 1 then p = p .. " -> " end
      p = p .. v
  end
  return p
end

-- Generate type information for all node ids in r
local function mkTypes(r)
  local vs = {}
  for i, _ in pairs(r) do
    vs[i] = nowdb.UINT
  end
  return vs
end

---------------------------------------------------------------------------
-- Produces a list of n randomly selected node ids
-- which appear as origin in 'edge';
-- nodes with less than l or more than u appearances in 'edge'
-- are ignored.
-- This helper function should go to a sampling library.
---------------------------------------------------------------------------
local function randomNodes(edge, n, r, l, u)
    local mx = r*n
    local res = {}
    for i = 1, n do
        res[#res+1] = i
    end

    math.randomseed(os.time())

    local stmt = string.format(
      [[select origin, count(*) from %s
         group by origin]], edge)

    local countme = 0
    local cur = nowdb.execute(stmt)
    for row in cur.rows() do
	countme = countme + 1
        local c = row.field(1)
        if c > l and c < u then
	   local x = countme
	   if x > n then
              x = math.random(mx)
	   end
           if x <= n then
              res[x] = row.field(0)
           end
        end
    end
    return res, nil
end

---------------------------------------------------------------------------
-- Stored Procedure to find the hortest Path 
--------------------------------------------
-- root  : starting node
-- target: end node
-- edge  : name of the edge we want to search
-- it    : number of iterations
---------------------------------------------------------------------------
function shortest(root, target, edge, it)
  if root == target then
     return nowdb.makeresult(nowdb.Uint, root)
  end
  local l, f, b = shortestInDB(root, target, edge, it)
  if l == -1 then
     return nowdb.raise(nowdb.EOF, "no path found")	  
  end
  local p1, p2 = linkPaths(root, target, l, f, b)
  if not p1 then p1 = {} end -- why are they nil?
  if not p2 then p2 = {} end
  local r = joinPaths(root, p1, l, p2, target)
  if not r then
     return nowdb.raise(nowdb.EOF, "no path found")	  
  end
  return nowdb.array2row(mkTypes(r), r)
end

---------------------------------------------------------------------------
-- Stored Procedure to find samples of nodes
-- that appear as origin in an edge
-- (the function does not belong here,
--  it should go to a stats package)
--------------------------------------------
-- edge  : name of the edge we want to search
-- n     : number of samples we want
-- r     : probability ratio (e.g.: 100 = 1 out of 100 is taken)
-- l     : lower bound of appearances in edge (fewer is ignored)
-- u     : upper bound of appearances in edge (more  is ignored)
---------------------------------------------------------------------------
function samplenodes(edge, n, r, l, u)
  local res = randomNodes(edge, n, r, l, u)
  if res then
     local row = nowdb.makerow()
     for _, v in pairs(res) do
         row.add2row(nowdb.UINT, v)
         row.closerow()
     end
     return row
  end
end

return path
