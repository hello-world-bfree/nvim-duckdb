---@class DuckDB
---Main module for DuckDB Neovim integration
local M = {}

local query_module = require('duckdb.query')
local ui_module = require('duckdb.ui')
local buffer_module = require('duckdb.buffer')
local ffi_module = require('duckdb.ffi')
local validate_module = require('duckdb.validate')

---@class DuckDBConfig
---@field max_rows number Maximum rows to display in results
---@field max_col_width number Maximum column width in display
---@field auto_close boolean Auto-close result window on selection
---@field default_format string Default export format (csv, json, table)

---Plugin configuration
---@type DuckDBConfig
M.config = {
  max_rows = 1000,
  max_col_width = 50,
  auto_close = false,
  default_format = 'table',
}

---Setup plugin with user configuration
---@param opts DuckDBConfig?
function M.setup(opts)
  M.config = vim.tbl_deep_extend('force', M.config, opts or {})

  -- Check if DuckDB is available
  local available, err = ffi_module.is_available()
  if not available then
    vim.notify(
      string.format('[DuckDB] %s', err),
      vim.log.levels.WARN
    )
  end
end

---Execute a SQL query on buffer(s)
---@param query string SQL query to execute
---@param opts table? Options
---  - buffer: Buffer identifier (default: current buffer)
---  - display: Display mode ("float", "split", "none") (default: "float")
---  - export: Export path (optional)
---  - format: Export format (optional, default: config.default_format)
---@return QueryResult? result
---@return string? error
function M.query(query, opts)
  opts = opts or {}

  -- Check if DuckDB is available
  local available, err = ffi_module.is_available()
  if not available then
    vim.notify(
      string.format('[DuckDB] %s', err),
      vim.log.levels.ERROR
    )
    return nil, err
  end

  -- Execute query
  local result, query_err = query_module.query_buffer(query, opts.buffer)

  if not result then
    vim.notify(
      string.format('[DuckDB] Query failed: %s', query_err),
      vim.log.levels.ERROR
    )
    return nil, query_err
  end

  -- Display results
  local display_mode = opts.display or "float"

  if display_mode == "float" then
    ui_module.display_results(result, {
      max_rows = M.config.max_rows,
      max_col_width = M.config.max_col_width,
      title = opts.title,
    })
  elseif display_mode == "split" then
    ui_module.results_to_buffer(result, {
      max_rows = M.config.max_rows,
      max_col_width = M.config.max_col_width,
    })
  end

  -- Export if requested
  if opts.export then
    local export_format = opts.format or M.config.default_format
    local success, export_err = ui_module.export_results(result, opts.export, export_format)

    if not success then
      vim.notify(
        string.format('[DuckDB] Export failed: %s', export_err),
        vim.log.levels.ERROR
      )
    else
      vim.notify(
        string.format('[DuckDB] Results exported to %s', opts.export),
        vim.log.levels.INFO
      )
    end
  end

  return result
end

---Query current buffer
---@param query string SQL query
---@param opts table? Options
---@return QueryResult? result
---@return string? error
function M.query_current_buffer(query, opts)
  opts = opts or {}
  opts.buffer = vim.api.nvim_get_current_buf()
  return M.query(query, opts)
end

---Interactive query prompt
---@param opts table? Options
function M.query_prompt(opts)
  opts = opts or {}

  vim.ui.input({
    prompt = 'DuckDB Query: ',
    default = 'SELECT * FROM buffer LIMIT 10',
  }, function(input)
    if input and input ~= '' then
      M.query(input, opts)
    end
  end)
end

---Query from visual selection
---@param opts table? Options
function M.query_visual(opts)
  opts = opts or {}

  -- Get visual selection
  local start_pos = vim.fn.getpos("'<")
  local end_pos = vim.fn.getpos("'>")
  local lines = vim.fn.getline(start_pos[2], end_pos[2])

  if #lines == 0 then
    vim.notify('[DuckDB] No text selected', vim.log.levels.WARN)
    return
  end

  -- Handle partial line selections
  if #lines == 1 then
    lines[1] = lines[1]:sub(start_pos[3], end_pos[3])
  else
    lines[1] = lines[1]:sub(start_pos[3])
    lines[#lines] = lines[#lines]:sub(1, end_pos[3])
  end

  local query = table.concat(lines, '\n')
  M.query(query, opts)
end

---Get buffer schema information
---@param identifier string|number|nil Buffer identifier
---@return table? schema
---@return string? error
function M.get_schema(identifier)
  local buffer_info, err = buffer_module.get_buffer_info(identifier)
  if not buffer_info then
    return nil, err
  end

  -- Execute query to get schema
  local query = "DESCRIBE buffer"
  local result, query_err = M.query(query, {
    buffer = identifier,
    display = "float",
    title = string.format(' Schema: %s ', buffer_info.name),
  })

  return result, query_err
end

---List all available buffers with data
---@return table buffers
function M.list_queryable_buffers()
  local buffers = {}

  for _, bufnr in ipairs(vim.api.nvim_list_bufs()) do
    if vim.api.nvim_buf_is_loaded(bufnr) then
      local name = vim.api.nvim_buf_get_name(bufnr)
      local filetype = vim.api.nvim_buf_get_option(bufnr, 'filetype')

      -- Check if buffer is a queryable type
      if filetype == 'csv' or filetype == 'json' or filetype == 'jsonl' or
         name:match('%.csv$') or name:match('%.json$') or name:match('%.jsonl$') then
        table.insert(buffers, {
          bufnr = bufnr,
          name = name,
          filetype = filetype,
        })
      end
    end
  end

  return buffers
end

---Execute a query and return results as Lua table
---@param query string SQL query
---@param buffer_id string|number|nil Buffer identifier
---@return table? rows Array of row objects (tables with column name keys)
---@return string? error
function M.query_as_table(query, buffer_id)
  local result, err = query_module.query_buffer(query, buffer_id)
  if not result then
    return nil, err
  end

  local rows = {}
  for _, row in ipairs(result.rows) do
    local obj = {}
    for i, col in ipairs(result.columns) do
      obj[col] = row[i]
    end
    table.insert(rows, obj)
  end

  return rows
end

---Create a command handler for :DuckDB
---@param args table Command arguments
function M.command_handler(args)
  local query = args.args

  if not query or query == '' then
    M.query_prompt()
    return
  end

  -- Parse options from range/mods
  local opts = {}

  -- Check if visual mode
  if args.range > 0 then
    -- Get visual selection
    local lines = vim.fn.getline(args.line1, args.line2)
    query = table.concat(lines, '\n')
  end

  M.query(query, opts)
end

---Setup SQL completion
---@return table completions
function M.get_sql_completions()
  local keywords = {
    'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT',
    'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN',
    'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT',
    'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
    'HAVING', 'UNION', 'INTERSECT', 'EXCEPT',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'NULL', 'IS NULL', 'IS NOT NULL',
    'ASC', 'DESC',
    'buffer', -- Special function
  }

  return keywords
end

---Validate buffer content using DuckDB's parser
---@param identifier string|number|nil Buffer identifier
---@param opts table? Options
---  - show_diagnostics: Show inline diagnostics (default: true)
---  - show_float: Show floating window with results (default: true)
---@return ValidationResult? result
---@return string? error
function M.validate(identifier, opts)
  opts = opts or {}
  local show_diagnostics = opts.show_diagnostics ~= false
  local show_float = opts.show_float ~= false

  -- Check if DuckDB is available
  local available, err = ffi_module.is_available()
  if not available then
    vim.notify(
      string.format('[DuckDB] %s', err),
      vim.log.levels.ERROR
    )
    return nil, err
  end

  -- Validate buffer
  local result, validate_err = validate_module.validate_buffer(identifier)

  if not result then
    vim.notify(
      string.format('[DuckDB] Validation failed: %s', validate_err),
      vim.log.levels.ERROR
    )
    return nil, validate_err
  end

  -- Get buffer info for display
  local buffer_info, _ = buffer_module.get_buffer_info(identifier)
  local bufnr = buffer_info and buffer_info.bufnr or vim.api.nvim_get_current_buf()
  local buffer_name = buffer_info and buffer_info.name or 'current buffer'

  -- Set diagnostics if requested
  if show_diagnostics then
    validate_module.set_diagnostics(bufnr, result)
  end

  -- Show floating window if requested
  if show_float then
    validate_module.display_validation_results(result, buffer_name)
  end

  -- Notify user
  if result.valid and #result.errors == 0 and #result.warnings == 0 then
    vim.notify('[DuckDB] Validation passed! ✓', vim.log.levels.INFO)
  elseif #result.errors > 0 then
    vim.notify(
      string.format('[DuckDB] Validation failed with %d error(s)', #result.errors),
      vim.log.levels.ERROR
    )
  else
    vim.notify(
      string.format('[DuckDB] Validation passed with %d warning(s)', #result.warnings),
      vim.log.levels.WARN
    )
  end

  return result
end

---Validate current buffer
---@param opts table? Options
---@return ValidationResult? result
---@return string? error
function M.validate_current_buffer(opts)
  return M.validate(nil, opts)
end

---Clear validation diagnostics for a buffer
---@param identifier string|number|nil Buffer identifier
function M.clear_validation(identifier)
  local buffer_info, _ = buffer_module.get_buffer_info(identifier)
  local bufnr = buffer_info and buffer_info.bufnr or vim.api.nvim_get_current_buf()
  validate_module.clear_diagnostics(bufnr)
  vim.notify('[DuckDB] Cleared validation diagnostics', vim.log.levels.INFO)
end

return M
