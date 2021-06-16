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

function path.shortest(root, target, arc, it)
  local forward  = {}
  local backward = {}
  local fnextgen = {root}
  local bnextgen = {target}
  local fprevgen = {}
  local bprevgen = {}
  local link = -1

  for i = 1, it do
      print("iteration " .. i)
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
		 print("FOUND target!")
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
		 print("FOUND forward!")
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
		 print("FOUND backward (" .. k .. "): " .. #backward[k])
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

function tshortest(root, target, link, it)
  print("hello path!")
  local link, f, b = path.shortest(root, target, link, it)
  local ff = 0
  local bb = 0
  for _ in pairs(f) do ff = ff + 1 end
  for _ in pairs(b) do bb = ff + 1 end
  print("FOUND: " .. link .. " (" .. ff .. ", " .. bb .. ")")
end

return path
