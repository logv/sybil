function map(count)
  return { count=count }
end

function reduce(results, new)
  results.count = (results.count or 0) + new.count
  return results

end

function finalize(results)
  print("LUA FINALIZING RESULTS")
  results["finalized"] = 1
  return results
end
