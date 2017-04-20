function map(count)
  local total = 0
  for i=1,count do
	local val = get_int(i, COLS.integer_time)
	if val then
	  total = total + val
	end
  end

  return { count=count, total = total }
end

function reduce(results, new)
  results.count = (results.count or 0) + (new.count or 1)
  results.total = (results.total or 0) + (new.total)
  return results

end

function finalize(results)
  print("LUA FINALIZING RESULTS")
  results["finalized"] = 1
  results["avg"] = results.total / results.count
  return results
end
