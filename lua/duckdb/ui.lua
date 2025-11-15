---@class DuckDBUI
local M = {}

---@class DisplayOptions
---@field max_rows number? Maximum rows to display (default: 1000)
---@field max_col_width number? Maximum column width (default: 50)
---@field title string? Window title

---Format a value for display
---@param value any
---@return string
local function format_value(value)
  if value == nil then
    return "NULL"
  elseif type(value) == "boolean" then
    return value and "true" or "false"
  elseif type(value) == "number" then
    -- Format numbers with reasonable precision
    if math.floor(value) == value then
      return tostring(value)
    else
      return string.format("%.4f", value):gsub("0+$", ""):gsub("%.$", "")
    end
  else
    return tostring(value)
  end
end

---Calculate column widths
---@param columns table<string>
---@param rows table<table>
---@param max_width number
---@return table<number>
local function calculate_column_widths(columns, rows, max_width)
  local widths = {}

  -- Initialize with column name lengths
  for i, col in ipairs(columns) do
    widths[i] = #col
  end

  -- Check row values
  for _, row in ipairs(rows) do
    for i, value in ipairs(row) do
      local formatted = format_value(value)
      widths[i] = math.max(widths[i], #formatted)
    end
  end

  -- Apply max width constraint
  for i = 1, #widths do
    widths[i] = math.min(widths[i], max_width)
  end

  return widths
end

---Format a row with proper padding
---@param values table<any>
---@param widths table<number>
---@return string
local function format_row(values, widths)
  local parts = {}
  for i, value in ipairs(values) do
    local formatted = format_value(value)
    -- Truncate if too long
    if #formatted > widths[i] then
      formatted = formatted:sub(1, widths[i] - 3) .. "..."
    end
    -- Pad to width
    formatted = formatted .. string.rep(" ", widths[i] - #formatted)
    table.insert(parts, formatted)
  end
  return "│ " .. table.concat(parts, " │ ") .. " │"
end

---Create separator line
---@param widths table<number>
---@param style string "top" | "middle" | "bottom"
---@return string
local function create_separator(widths, style)
  local left, middle, right, horizontal

  if style == "top" then
    left, middle, right, horizontal = "┌", "┬", "┐", "─"
  elseif style == "middle" then
    left, middle, right, horizontal = "├", "┼", "┤", "─"
  else -- bottom
    left, middle, right, horizontal = "└", "┴", "┘", "─"
  end

  local parts = {}
  for _, width in ipairs(widths) do
    table.insert(parts, string.rep(horizontal, width + 2))
  end

  return left .. table.concat(parts, middle) .. right
end

---Format query results as a table
---@param result QueryResult
---@param options DisplayOptions?
---@return table<string> lines
function M.format_results(result, options)
  options = options or {}
  local max_rows = options.max_rows or 1000
  local max_col_width = options.max_col_width or 50

  local lines = {}

  if result.row_count == 0 then
    if result.rows_changed > 0 then
      table.insert(lines, string.format("Query OK, %d row(s) affected", result.rows_changed))
    else
      table.insert(lines, "Empty result set")
    end
    return lines
  end

  -- Limit rows for display
  local display_rows = {}
  local truncated = false
  for i = 1, math.min(result.row_count, max_rows) do
    table.insert(display_rows, result.rows[i])
  end
  if result.row_count > max_rows then
    truncated = true
  end

  -- Calculate column widths
  local widths = calculate_column_widths(result.columns, display_rows, max_col_width)

  -- Top border
  table.insert(lines, create_separator(widths, "top"))

  -- Header row
  table.insert(lines, format_row(result.columns, widths))

  -- Header separator
  table.insert(lines, create_separator(widths, "middle"))

  -- Data rows
  for _, row in ipairs(display_rows) do
    table.insert(lines, format_row(row, widths))
  end

  -- Bottom border
  table.insert(lines, create_separator(widths, "bottom"))

  -- Footer
  if truncated then
    table.insert(lines, "")
    table.insert(lines, string.format("Showing %d of %d rows", max_rows, result.row_count))
  else
    table.insert(lines, "")
    table.insert(lines, string.format("%d row(s) returned", result.row_count))
  end

  return lines
end

---Display results in a floating window
---@param result QueryResult
---@param options DisplayOptions?
function M.display_results(result, options)
  options = options or {}

  -- Format results
  local lines = M.format_results(result, options)

  -- Create buffer
  local buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_buf_set_option(buf, 'modifiable', false)
  vim.api.nvim_buf_set_option(buf, 'bufhidden', 'wipe')
  vim.api.nvim_buf_set_option(buf, 'filetype', 'duckdb-result')

  -- Calculate window size
  local max_width = 0
  for _, line in ipairs(lines) do
    max_width = math.max(max_width, #line)
  end

  local width = math.min(max_width + 2, vim.o.columns - 4)
  local height = math.min(#lines, vim.o.lines - 4)

  -- Calculate position (centered)
  local row = math.floor((vim.o.lines - height) / 2)
  local col = math.floor((vim.o.columns - width) / 2)

  -- Window options
  local win_opts = {
    relative = 'editor',
    width = width,
    height = height,
    row = row,
    col = col,
    style = 'minimal',
    border = 'rounded',
    title = options.title or ' DuckDB Results ',
    title_pos = 'center',
  }

  -- Create window
  local win = vim.api.nvim_open_win(buf, true, win_opts)

  -- Set window options
  vim.api.nvim_win_set_option(win, 'wrap', false)
  vim.api.nvim_win_set_option(win, 'cursorline', true)

  -- Key mappings
  local keymaps = {
    { 'n', 'q', '<cmd>close<cr>', { buffer = buf, nowait = true, silent = true } },
    { 'n', '<Esc>', '<cmd>close<cr>', { buffer = buf, nowait = true, silent = true } },
  }

  for _, keymap in ipairs(keymaps) do
    vim.keymap.set(keymap[1], keymap[2], keymap[3], keymap[4])
  end
end

---Write results to a new buffer
---@param result QueryResult
---@param options DisplayOptions?
---@return number bufnr
function M.results_to_buffer(result, options)
  options = options or {}

  -- Format results
  local lines = M.format_results(result, options)

  -- Create new buffer
  local buf = vim.api.nvim_create_buf(true, false)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_buf_set_option(buf, 'filetype', 'duckdb-result')
  vim.api.nvim_buf_set_name(buf, 'DuckDB Results')

  -- Open in new split
  vim.cmd('split')
  vim.api.nvim_win_set_buf(0, buf)

  return buf
end

---Format results as CSV
---@param result QueryResult
---@return table<string> lines
function M.format_as_csv(result)
  local lines = {}

  -- Header
  table.insert(lines, table.concat(result.columns, ','))

  -- Rows
  for _, row in ipairs(result.rows) do
    local values = {}
    for _, value in ipairs(row) do
      local formatted = format_value(value)
      -- Escape quotes and wrap in quotes if contains comma, quote, or newline
      if formatted:match('[,"\n]') then
        formatted = '"' .. formatted:gsub('"', '""') .. '"'
      end
      table.insert(values, formatted)
    end
    table.insert(lines, table.concat(values, ','))
  end

  return lines
end

---Format results as JSON
---@param result QueryResult
---@return string json
function M.format_as_json(result)
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

---Export results to a file
---@param result QueryResult
---@param filepath string
---@param format string "csv" | "json" | "table"
---@return boolean success
---@return string? error
function M.export_results(result, filepath, format)
  local lines

  if format == 'csv' then
    lines = M.format_as_csv(result)
  elseif format == 'json' then
    lines = { M.format_as_json(result) }
  else
    lines = M.format_results(result)
  end

  local file, err = io.open(filepath, 'w')
  if not file then
    return false, err
  end

  for _, line in ipairs(lines) do
    file:write(line .. '\n')
  end

  file:close()
  return true
end

return M
