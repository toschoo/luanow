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

local function nextgen(keys, edge)
  return string.format(
    [[select origin, destin
        from %s
       where origin in %s]],
    edge, toinstr(keys))
end

function path.shortest(root, target, link, it)
  fnextgen = {root}
  bnextgen = {target}
  for i = 1, it do
      print("iteration " .. i)
      for k, keys in chunker(fnextgen, path.CHUNKSZ) do
          local stmt = nextgen(keys, link)
          -- print(stmt)
          local cur = nowdb.execute(stmt)
          for row in cur.rows() do
              print(row.field(0) .. " -> " .. row.field(1))
          end
          cur.release()
      end
  end
end 

function tshortest(root, target, link, it)
  print("hello path!")
  return path.shortest(root, target, link, it)
end

return path
