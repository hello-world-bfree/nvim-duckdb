---@class DuckDBHistory
local M = {}

local data_path = vim.fn.stdpath("data") .. "/duckdb_history.json"

---@class HistoryEntry
---@field query string The SQL query
---@field timestamp number Unix timestamp
---@field row_count number Number of rows returned
---@field execution_time_ms number Execution time in milliseconds
---@field buffer_name string? Name of the buffer queried

---@type HistoryEntry[]
local history_cache = nil

local function ensure_cache()
  if history_cache then
    return
  end
  local file = io.open(data_path, "r")
  if file then
    local content = file:read("*a")
    file:close()
    local ok, data = pcall(vim.json.decode, content)
    if ok and type(data) == "table" then
      history_cache = data
      return
    end
  end
  history_cache = {}
end

local function save_history()
  local file = io.open(data_path, "w")
  if file then
    file:write(vim.json.encode(history_cache))
    file:close()
  end
end

---Add a query to history
---@param entry HistoryEntry
---@param limit number? Maximum entries to keep (default: 500)
function M.add(entry, limit)
  limit = limit or 500
  ensure_cache()
  table.insert(history_cache, 1, entry)
  while #history_cache > limit do
    table.remove(history_cache)
  end
  save_history()
end

---Get history entries
---@param opts table? Options: limit, search
---@return HistoryEntry[]
function M.get(opts)
  opts = opts or {}
  ensure_cache()

  local result = history_cache
  if opts.search and opts.search ~= "" then
    local pattern = opts.search:lower()
    result = vim.tbl_filter(function(e)
      return e.query:lower():find(pattern, 1, true) ~= nil
    end, result)
  end

  if opts.limit and opts.limit > 0 then
    local limited = {}
    for i = 1, math.min(opts.limit, #result) do
      limited[i] = result[i]
    end
    return limited
  end

  return result
end

---Search history by query text
---@param pattern string Search pattern
---@param limit number? Max results
---@return HistoryEntry[]
function M.search(pattern, limit)
  return M.get({ search = pattern, limit = limit })
end

---Clear all history
function M.clear()
  history_cache = {}
  save_history()
end

---Get the last N queries
---@param n number
---@return HistoryEntry[]
function M.recent(n)
  return M.get({ limit = n })
end

---Display history in a floating picker
---@param on_select function? Callback when entry is selected
function M.show_picker(on_select)
  ensure_cache()

  if #history_cache == 0 then
    vim.notify("[DuckDB] No query history", vim.log.levels.INFO)
    return
  end

  local items = {}
  for i, entry in ipairs(history_cache) do
    local time_str = os.date("%Y-%m-%d %H:%M", entry.timestamp)
    local query_preview = entry.query:gsub("\n", " "):sub(1, 60)
    if #entry.query > 60 then
      query_preview = query_preview .. "..."
    end
    table.insert(items, {
      idx = i,
      display = string.format("[%s] %s (%d rows)", time_str, query_preview, entry.row_count or 0),
      entry = entry,
    })
  end

  vim.ui.select(items, {
    prompt = "Query History:",
    format_item = function(item)
      return item.display
    end,
  }, function(choice)
    if choice and on_select then
      on_select(choice.entry)
    elseif choice then
      local duckdb = require("duckdb")
      duckdb.query(choice.entry.query)
    end
  end)
end

return M
