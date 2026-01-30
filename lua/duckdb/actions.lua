---@class DuckDBActions
local M = {}

---@class ResultMetadata
---@field query string Original query
---@field result QueryResult The query result
---@field buffer_name string? Source buffer name
---@field timestamp number When query was executed


---Get column index from cursor position in result buffer
---@param bufnr number
---@return number? col_idx 1-based column index
---@return string? col_name Column name
local function get_column_at_cursor(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.result then
    return nil, nil
  end

  local cursor = vim.api.nvim_win_get_cursor(0)
  local line = vim.api.nvim_buf_get_lines(bufnr, cursor[1] - 1, cursor[1], false)[1]
  if not line then
    return nil, nil
  end

  local col_pos = cursor[2] + 1
  local current_col = 0
  local in_col = false
  local col_start = 0

  for i = 1, #line do
    local char = line:sub(i, i)
    if char == "│" then
      if in_col and col_pos >= col_start and col_pos < i then
        if current_col >= 1 and current_col <= #metadata.result.columns then
          return current_col, metadata.result.columns[current_col]
        end
      end
      current_col = current_col + 1
      col_start = i + 1
      in_col = true
    end
  end

  if in_col and col_pos >= col_start then
    if current_col >= 1 and current_col <= #metadata.result.columns then
      return current_col, metadata.result.columns[current_col]
    end
  end

  return nil, nil
end

---Format result as JSON array
---@param result QueryResult
---@return string
function M.format_json_array(result)
  local rows = {}
  for _, row in ipairs(result.rows) do
    local obj = {}
    for i, col in ipairs(result.columns) do
      obj[col] = row[i]
    end
    table.insert(rows, obj)
  end
  return vim.json.encode(rows)
end

---Format result as JSON object keyed by first column
---@param result QueryResult
---@return string
function M.format_json_object(result)
  if #result.columns < 1 then
    return "{}"
  end
  local obj = {}
  for _, row in ipairs(result.rows) do
    local key = tostring(row[1])
    local val = {}
    for i = 2, #result.columns do
      val[result.columns[i]] = row[i]
    end
    obj[key] = val
  end
  return vim.json.encode(obj)
end

---Format result as single JSON object (first row only)
---@param result QueryResult
---@return string
function M.format_json_single(result)
  if #result.rows == 0 then
    return "{}"
  end
  local obj = {}
  for i, col in ipairs(result.columns) do
    obj[col] = result.rows[1][i]
  end
  return vim.json.encode(obj)
end

---Format result as CSV
---@param result QueryResult
---@return string
function M.format_csv(result)
  local ui = require("duckdb.ui")
  local lines = ui.format_as_csv(result)
  return table.concat(lines, "\n")
end

---Yank result as JSON array
---@param bufnr number
function M.yank_json_array(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.result then
    vim.notify("[DuckDB] No result data", vim.log.levels.WARN)
    return
  end
  local json = M.format_json_array(metadata.result)
  vim.fn.setreg("+", json)
  vim.fn.setreg('"', json)
  vim.notify("[DuckDB] Copied as JSON array", vim.log.levels.INFO)
end

---Yank result with format selection
---@param bufnr number
function M.yank_json_select(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.result then
    vim.notify("[DuckDB] No result data", vim.log.levels.WARN)
    return
  end

  vim.ui.select({ "Array", "Object (keyed by first column)", "Single (first row)" }, {
    prompt = "JSON Format:",
  }, function(choice)
    if not choice then
      return
    end
    local json
    if choice:find("Array") then
      json = M.format_json_array(metadata.result)
    elseif choice:find("Object") then
      json = M.format_json_object(metadata.result)
    else
      json = M.format_json_single(metadata.result)
    end
    vim.fn.setreg("+", json)
    vim.fn.setreg('"', json)
    vim.notify("[DuckDB] Copied as JSON", vim.log.levels.INFO)
  end)
end

---Yank result as CSV
---@param bufnr number
function M.yank_csv(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.result then
    vim.notify("[DuckDB] No result data", vim.log.levels.WARN)
    return
  end
  local csv = M.format_csv(metadata.result)
  vim.fn.setreg("+", csv)
  vim.fn.setreg('"', csv)
  vim.notify("[DuckDB] Copied as CSV", vim.log.levels.INFO)
end

---Filter results by adding WHERE clause
---@param bufnr number
function M.filter(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.query then
    vim.notify("[DuckDB] No query to filter", vim.log.levels.WARN)
    return
  end

  vim.ui.input({ prompt = "WHERE clause: " }, function(clause)
    if not clause or clause == "" then
      return
    end

    local original = metadata.query
    local new_query

    if original:lower():find("where") then
      new_query = original:gsub("([Ww][Hh][Ee][Rr][Ee]%s+)", "%1(" .. clause .. ") AND (") .. ")"
    else
      local from_pos = original:lower():find("from")
      if from_pos then
        local limit_pos = original:lower():find("limit")
        local group_pos = original:lower():find("group%s+by")
        local order_pos = original:lower():find("order%s+by")
        local insert_before = limit_pos or group_pos or order_pos

        if insert_before then
          new_query = original:sub(1, insert_before - 1) .. " WHERE " .. clause .. " " .. original:sub(insert_before)
        else
          new_query = original .. " WHERE " .. clause
        end
      else
        new_query = original .. " WHERE " .. clause
      end
    end

    local duckdb = require("duckdb")
    duckdb.query(new_query)
  end)
end

---Sort results by column under cursor
---@param bufnr number
function M.sort(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.query then
    vim.notify("[DuckDB] No query to sort", vim.log.levels.WARN)
    return
  end

  local _, col_name = get_column_at_cursor(bufnr)
  if not col_name then
    vim.ui.select(metadata.result.columns, {
      prompt = "Sort by column:",
    }, function(choice)
      if choice then
        M._apply_sort(metadata.query, choice)
      end
    end)
    return
  end

  vim.ui.select({ "ASC", "DESC" }, {
    prompt = string.format("Sort %s:", col_name),
  }, function(direction)
    if direction then
      M._apply_sort(metadata.query, col_name, direction)
    end
  end)
end

---Apply sort to query and execute
---@param query string
---@param column string
---@param direction string?
function M._apply_sort(query, column, direction)
  direction = direction or "ASC"

  local quoted_col = '"' .. column:gsub('"', '""') .. '"'
  local new_query

  if query:lower():find("order%s+by") then
    new_query = query:gsub("([Oo][Rr][Dd][Ee][Rr]%s+[Bb][Yy]%s+)[^;]+", "%1" .. quoted_col .. " " .. direction)
  else
    local limit_match = query:lower():find("limit")
    if limit_match then
      new_query = query:sub(1, limit_match - 1) .. " ORDER BY " .. quoted_col .. " " .. direction .. " " .. query:sub(limit_match)
    else
      new_query = query .. " ORDER BY " .. quoted_col .. " " .. direction
    end
  end

  local duckdb = require("duckdb")
  duckdb.query(new_query)
end

---Re-run the original query
---@param bufnr number
function M.rerun(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.query then
    vim.notify("[DuckDB] No query to re-run", vim.log.levels.WARN)
    return
  end

  local duckdb = require("duckdb")
  duckdb.query(metadata.query)
end

---Export results to file
---@param bufnr number
function M.export(bufnr)
  local metadata = vim.b[bufnr].duckdb_metadata
  if not metadata or not metadata.result then
    vim.notify("[DuckDB] No result data", vim.log.levels.WARN)
    return
  end

  vim.ui.select({ "csv", "json", "table" }, {
    prompt = "Export format:",
  }, function(format)
    if not format then
      return
    end

    vim.ui.input({
      prompt = "Export path: ",
      default = vim.fn.getcwd() .. "/export." .. format,
      completion = "file",
    }, function(path)
      if not path or path == "" then
        return
      end

      local ui = require("duckdb.ui")
      local success, err = ui.export_results(metadata.result, path, format)
      if success then
        vim.notify("[DuckDB] Exported to " .. path, vim.log.levels.INFO)
      else
        vim.notify("[DuckDB] Export failed: " .. (err or "unknown"), vim.log.levels.ERROR)
      end
    end)
  end)
end

---Setup keymaps for a result buffer
---@param bufnr number
---@param metadata ResultMetadata
function M.setup_keymaps(bufnr, metadata)
  vim.b[bufnr].duckdb_metadata = metadata

  local opts = { buffer = bufnr, nowait = true, silent = true }

  vim.keymap.set("n", "f", function()
    M.filter(bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Filter results" }))

  vim.keymap.set("n", "s", function()
    M.sort(bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Sort by column" }))

  vim.keymap.set("n", "ya", function()
    M.yank_json_array(bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Yank as JSON array" }))

  vim.keymap.set("n", "yo", function()
    M.yank_json_select(bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Yank JSON (choose format)" }))

  vim.keymap.set("n", "yc", function()
    M.yank_csv(bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Yank as CSV" }))

  vim.keymap.set("n", "e", function()
    M.export(bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Export to file" }))

  vim.keymap.set("n", "r", function()
    M.rerun(bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Re-run query" }))
end

return M
