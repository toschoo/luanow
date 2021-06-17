local path = {}

path.CHUNKSZ = 100000

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

local function toinstr(keys)
  s = "("
  for i, k in pairs(keys) do
    if i > 1 then s = s .. ", " end
    s = s .. k
  end
  return s .. ")"
end

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
      local tmp = {}
      for k, keys in chunker(fnextgen, path.CHUNKSZ) do
          local stmt = nextgen(keys, arc, true)
          -- print(stmt)
          local cur = nowdb.execute(stmt)
          for row in cur.rows() do
              -- print(row.field(0) .. " -> " .. row.field(1))
	      k = row.field(0)
	      n = row.field(1)
	      if forward[n] then
                 forward[n][#forward[n]+1] = k
	      else
                 forward[n] = {k}
	      end
	      if n == target then
	         link = n
		 break
	      end
	      if not fprevgen[n] then tmp[n] = true end
          end
          cur.release()
	  if link ~= -1 then break end
	  for _, k in pairs(fnextgen) do
              fprevgen[k] = true
          end
	  fnextgen = {}
	  for k, _ in pairs(tmp) do
	      fnextgen[#fnextgen+1] = k
	  end
	  for k, _ in pairs(forward) do
              if backward[k] then
                 link = k
		 break
	      end
          end
	  if link ~= -1 then break end
      end
      local tmp = {}
      for k, keys in chunker(bnextgen, path.CHUNKSZ) do
          local stmt = nextgen(keys, arc, false)
          -- print(stmt)
          local cur = nowdb.execute(stmt)
          for row in cur.rows() do
              -- print(row.field(0) .. " <- " .. row.field(1))
	      local k = row.field(0)
	      local n = row.field(1)
	      if backward[k] then
                 backward[k][#backward[k]+1] = n
	      else
                 backward[k] = {n}
	      end
	      if not bprevgen[k] then tmp[k] = true end
          end
          cur.release()
	  if link ~= -1 then break end
	  for _, k in pairs(bnextgen) do
              bprevgen[k] = true
          end
	  bnextgen = {}
	  for k, _ in pairs(tmp) do
	      bnextgen[#bnextgen+1] = k
	  end
	  for k, _ in pairs(forward) do
              if backward[k] then
                 link = k
		 break
	      end
          end
	  if link ~= -1 then break end
      end
      if link ~= -1 then break end
  end
  return link, forward, backward
end 

local function findPath(root, neighbours, graph, p, seen, it)
  if it > 25 then return {} end
  for _, k in pairs(neighbours) do
      if not seen[k] then
         seen[k] = true 
         if k == root then return p end
         p[#p+1] = k
         rc = findPath(root, graph[k], graph, p, seen, it+1)
         if rc then return rc end
	 p[#p] = nil
      end
  end
  return false
end

local function linkPaths(root, target, link, forward, backward)
  if root == target then return {root} end
  local p1 = {}
  local p2 = {}
  findPath(root  , forward[link] , forward , p1, {}, 0)
  findPath(target, backward[link], backward, p2, {}, 0)
  return p1, p2
end

local function joinPaths(root, p1, link, p2, target)
  local r = {root}
  for i = #p1, 1, -1 do
      r[#r+1] = p1[i]
  end
  r[#r+1] = link
  for _, v in pairs(p2) do
      r[#r+1] = v
  end
  r[#r+1] = target
  return r
end

local function showPath(mypath)
  local p = ""
  for i, v in pairs(mypath) do
      if i > 1 then p = p .. " -> " end
      p = p .. v
  end
  return p
end

local function mkTypes(r)
  local vs = {}
  for i, _ in pairs(r) do
    vs[i] = nowdb.UINT
  end
  return vs
end

function shortest(root, target, link, it)
  local l, f, b = shortestInDB(root, target, link, it)
  local p1, p2 = linkPaths(root, target, l, f, b)
  local r = joinPaths(root, p1, l, p2, target)
  -- print(showPath(r))
  return nowdb.array2row(mkTypes(r), r)
end

return path
