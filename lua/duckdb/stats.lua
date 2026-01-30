---@class DuckDBStats
local M = {}

---@class ColumnStats
---@field name string Column name
---@field type string Column type
---@field count number Total count
---@field null_count number Number of NULLs
---@field null_pct number NULL percentage
---@field unique_count number? Number of unique values
---@field min any? Minimum value
---@field max any? Maximum value
---@field avg number? Average (numeric only)

---@class SchemaStats
---@field columns ColumnStats[]
---@field row_count number
---@field timestamp number Cache timestamp

local stats_cache = {}
local CACHE_TTL_SECONDS = 300

---Get cached stats for a buffer
---@param bufnr number
---@return SchemaStats?
local function get_cached(bufnr)
  local cached = stats_cache[bufnr]
  if cached and (os.time() - cached.timestamp) < CACHE_TTL_SECONDS then
    return cached
  end
  return nil
end

---Parse SUMMARIZE output into structured stats
---@param result QueryResult
---@return ColumnStats[]
local function parse_summarize_result(result)
  local columns = {}

  for _, row in ipairs(result.rows) do
    local stats = {}
    for i, col in ipairs(result.columns) do
      stats[col:lower()] = row[i]
    end

    local col_stats = {
      name = stats["column_name"] or stats["name"] or "unknown",
      type = stats["column_type"] or stats["type"] or "unknown",
      count = tonumber(stats["count"]) or 0,
      null_count = tonumber(stats["null_percentage"]) and
        math.floor((tonumber(stats["null_percentage"]) or 0) / 100 * (tonumber(stats["count"]) or 0)) or 0,
      null_pct = tonumber(stats["null_percentage"]) or 0,
      unique_count = tonumber(stats["unique"]) or tonumber(stats["approx_unique"]),
      min = stats["min"],
      max = stats["max"],
      avg = tonumber(stats["avg"]),
    }

    table.insert(columns, col_stats)
  end

  return columns
end

---Get schema statistics for a buffer using SUMMARIZE
---@param buffer_id string|number|nil
---@return SchemaStats?
---@return string? error
function M.get_stats(buffer_id)
  local buffer_module = require("duckdb.buffer")
  local buffer_info, err = buffer_module.get_buffer_info(buffer_id)
  if not buffer_info then
    return nil, err
  end

  local cached = get_cached(buffer_info.bufnr)
  if cached then
    return cached
  end

  local query_module = require("duckdb.query")
  local result, query_err = query_module.query_buffer("SUMMARIZE SELECT * FROM buffer", buffer_id)
  if not result then
    return nil, query_err
  end

  local columns = parse_summarize_result(result)

  local count_result = query_module.query_buffer("SELECT COUNT(*) as cnt FROM buffer", buffer_id)
  local row_count = 0
  if count_result and #count_result.rows > 0 then
    row_count = tonumber(count_result.rows[1][1]) or 0
  end

  local stats = {
    columns = columns,
    row_count = row_count,
    timestamp = os.time(),
  }

  stats_cache[buffer_info.bufnr] = stats
  return stats
end

---Get stats for a specific column
---@param buffer_id string|number|nil
---@param column_name string
---@return ColumnStats?
function M.get_column_stats(buffer_id, column_name)
  local stats = M.get_stats(buffer_id)
  if not stats then
    return nil
  end

  for _, col in ipairs(stats.columns) do
    if col.name == column_name then
      return col
    end
  end
  return nil
end

---Format stats for display
---@param stats ColumnStats
---@return string[]
function M.format_column_stats(stats)
  local lines = {
    string.format("Column: %s", stats.name),
    string.format("Type: %s", stats.type),
    string.format("Count: %d", stats.count),
    string.format("NULLs: %d (%.1f%%)", stats.null_count, stats.null_pct),
  }

  if stats.unique_count then
    table.insert(lines, string.format("Unique: %d", stats.unique_count))
  end

  if stats.min ~= nil then
    table.insert(lines, string.format("Min: %s", tostring(stats.min)))
  end

  if stats.max ~= nil then
    table.insert(lines, string.format("Max: %s", tostring(stats.max)))
  end

  if stats.avg then
    table.insert(lines, string.format("Avg: %.4f", stats.avg))
  end

  return lines
end

---Show hover popup with column stats
---@param buffer_id string|number|nil
---@param column_name string?
function M.show_hover(buffer_id, column_name)
  if not column_name then
    local cursor_line = vim.api.nvim_get_current_line()
    column_name = cursor_line:match("^([^,]+)") or cursor_line:match("^%s*(.-)%s*$")
    if column_name then
      column_name = column_name:gsub("^%s*", ""):gsub("%s*$", "")
    end
  end

  if not column_name or column_name == "" then
    vim.notify("[DuckDB] No column name at cursor", vim.log.levels.WARN)
    return
  end

  local col_stats = M.get_column_stats(buffer_id, column_name)
  if not col_stats then
    local stats = M.get_stats(buffer_id)
    if stats and #stats.columns > 0 then
      local names = vim.tbl_map(function(c) return c.name end, stats.columns)
      vim.notify("[DuckDB] Column not found. Available: " .. table.concat(names, ", "), vim.log.levels.WARN)
    else
      vim.notify("[DuckDB] No stats found for column: " .. column_name, vim.log.levels.WARN)
    end
    return
  end

  local lines = M.format_column_stats(col_stats)
  vim.lsp.util.open_floating_preview(lines, "markdown", {
    border = "rounded",
    focus = false,
  })
end

---Clear stats cache for a buffer
---@param bufnr number?
function M.clear_cache(bufnr)
  if bufnr then
    stats_cache[bufnr] = nil
  else
    stats_cache = {}
  end
end

---Format full schema stats as lines for display
---@param stats SchemaStats
---@return string[]
function M.format_schema_stats(stats)
  local lines = {
    string.format("Total Rows: %d", stats.row_count),
    string.format("Columns: %d", #stats.columns),
    "",
  }

  for _, col in ipairs(stats.columns) do
    table.insert(lines, string.format("  %s (%s)", col.name, col.type))
    if col.null_pct > 0 then
      table.insert(lines, string.format("    NULLs: %.1f%%", col.null_pct))
    end
    if col.unique_count then
      table.insert(lines, string.format("    Unique: %d", col.unique_count))
    end
  end

  return lines
end

return M
