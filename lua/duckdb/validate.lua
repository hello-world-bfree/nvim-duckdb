---@class DuckDBValidate
---Validation module that leverages DuckDB's excellent error reporting
local M = {}

local query_module = require('duckdb.query')
local buffer_module = require('duckdb.buffer')

---@class ValidationError
---@field line number|nil Line number where error occurred
---@field column number|nil Column number where error occurred
---@field message string Error message
---@field severity string "error" | "warning" | "info"
---@field error_type string Type of error (e.g., "parse", "type", "schema")

---@class ValidationResult
---@field valid boolean Whether the buffer is valid
---@field errors table<ValidationError> List of validation errors
---@field warnings table<ValidationError> List of warnings
---@field info string|nil Additional information

---Parse DuckDB error message to extract line/column information
---@param error_msg string Error message from DuckDB
---@param format string File format (csv, json, jsonl)
---@return table<ValidationError> errors
local function parse_duckdb_error(error_msg, format)
  local errors = {}

  -- Common patterns in DuckDB error messages
  local patterns = {
    -- CSV line number patterns
    { pattern = "line (%d+)", type = "line" },
    { pattern = "on line (%d+)", type = "line" },
    { pattern = "at line (%d+)", type = "line" },
    { pattern = "row (%d+)", type = "line" },

    -- CSV column patterns
    { pattern = "column (%d+)", type = "column" },
    { pattern = "field (%d+)", type = "column" },

    -- JSON line patterns
    { pattern = "byte (%d+)", type = "byte" },
    { pattern = "position (%d+)", type = "position" },
  }

  local line_num = nil
  local col_num = nil

  -- Try to extract line/column numbers
  for _, pat in ipairs(patterns) do
    local match = error_msg:match(pat.pattern)
    if match then
      local num = tonumber(match)
      if pat.type == "line" or pat.type == "row" then
        line_num = num
      elseif pat.type == "column" or pat.type == "field" then
        col_num = num
      elseif pat.type == "byte" or pat.type == "position" then
        -- For JSON, try to convert byte position to line number
        -- This is approximate and would need the actual content
        line_num = num
      end
    end
  end

  -- Determine error type based on message content
  local error_type = "parse"
  if error_msg:match("[Tt]ype") or error_msg:match("[Cc]onversion") then
    error_type = "type"
  elseif error_msg:match("[Ss]chema") or error_msg:match("[Cc]olumn") then
    error_type = "schema"
  elseif error_msg:match("[Qq]uote") or error_msg:match("[Dd]elimiter") then
    error_type = "format"
  elseif error_msg:match("JSON") or error_msg:match("json") then
    error_type = "json"
  end

  table.insert(errors, {
    line = line_num,
    column = col_num,
    message = error_msg,
    severity = "error",
    error_type = error_type,
  })

  return errors
end

---Try to parse content with DuckDB and capture detailed errors
---@param content string Buffer content
---@param format string File format
---@return boolean success
---@return string? error_message
local function try_parse_with_duckdb(content, format)
  local conn, err = query_module.create_connection()
  if not conn then
    return false, "Failed to create DuckDB connection: " .. (err or "unknown error")
  end

  local success = false
  local error_message = nil

  -- Wrap in pcall to catch any errors
  local ok = pcall(function()
    local query

    if format == 'csv' then
      -- Use DuckDB's read_csv with strict settings to catch errors
      query = string.format(
        "SELECT * FROM read_csv(%s, auto_detect=true, sample_size=-1, ignore_errors=false, all_varchar=false)",
        vim.inspect(content)
      )
    elseif format == 'json' then
      -- Use DuckDB's read_json with strict settings
      query = string.format(
        "SELECT * FROM read_json(%s, auto_detect=true, format='auto')",
        vim.inspect(content)
      )
    elseif format == 'jsonl' then
      -- Use DuckDB's read_json for newline-delimited JSON
      query = string.format(
        "SELECT * FROM read_json(%s, auto_detect=true, format='newline_delimited')",
        vim.inspect(content)
      )
    else
      error("Unsupported format: " .. format)
    end

    -- Try to execute the query
    local result, query_err = query_module.execute_query(conn, query)

    if result then
      success = true
    else
      error_message = query_err
    end
  end)

  query_module.close_connection(conn)

  if not ok then
    return false, "Parsing failed"
  end

  return success, error_message
end

---Validate CSV content and provide detailed error information
---@param content string CSV content
---@param bufnr number Buffer number for context
---@return ValidationResult result
local function validate_csv(content, bufnr)
  local result = {
    valid = true,
    errors = {},
    warnings = {},
  }

  -- Basic checks first
  local lines = vim.split(content, '\n', { plain = true })

  if #lines == 0 then
    table.insert(result.errors, {
      line = 1,
      message = "CSV file is empty",
      severity = "error",
      error_type = "schema",
    })
    result.valid = false
    return result
  end

  -- Try parsing with DuckDB
  local success, error_msg = try_parse_with_duckdb(content, 'csv')

  if not success and error_msg then
    result.valid = false
    local parsed_errors = parse_duckdb_error(error_msg, 'csv')
    for _, err in ipairs(parsed_errors) do
      table.insert(result.errors, err)
    end
  end

  -- Additional CSV-specific validations
  if result.valid then
    -- Check for inconsistent column counts
    local header_cols = #vim.split(lines[1], ',', { plain = true })

    for i = 2, math.min(#lines, 100) do  -- Check first 100 lines
      if lines[i]:match('%S') then  -- Skip empty lines
        local cols = #vim.split(lines[i], ',', { plain = true })
        if cols ~= header_cols then
          table.insert(result.warnings, {
            line = i,
            message = string.format(
              "Inconsistent column count: expected %d columns, found %d",
              header_cols,
              cols
            ),
            severity = "warning",
            error_type = "schema",
          })
        end
      end
    end

    -- Check for potential encoding issues
    for i, line in ipairs(lines) do
      if i > 100 then break end  -- Check first 100 lines
      if line:match('[\0-\8\11-\12\14-\31]') then
        table.insert(result.warnings, {
          line = i,
          message = "Potential binary or control characters detected",
          severity = "warning",
          error_type = "format",
        })
      end
    end
  end

  result.info = string.format(
    "CSV with %d lines, %d errors, %d warnings",
    #lines,
    #result.errors,
    #result.warnings
  )

  return result
end

---Validate JSON content
---@param content string JSON content
---@param bufnr number Buffer number
---@return ValidationResult result
local function validate_json(content, bufnr)
  local result = {
    valid = true,
    errors = {},
    warnings = {},
  }

  -- Try Neovim's JSON parser first (faster for syntax errors)
  local ok, decode_result = pcall(vim.json.decode, content)

  if not ok then
    result.valid = false

    -- Try to extract line information from Vim's JSON error
    local error_msg = tostring(decode_result)
    local line_num = error_msg:match("line (%d+)")

    table.insert(result.errors, {
      line = line_num and tonumber(line_num) or nil,
      message = error_msg,
      severity = "error",
      error_type = "json",
    })
  else
    -- Validate with DuckDB for schema issues
    local success, error_msg = try_parse_with_duckdb(content, 'json')

    if not success and error_msg then
      result.valid = false
      local parsed_errors = parse_duckdb_error(error_msg, 'json')
      for _, err in ipairs(parsed_errors) do
        table.insert(result.errors, err)
      end
    end

    -- Check if it's an array (expected for querying)
    if result.valid and type(decode_result) ~= 'table' then
      table.insert(result.warnings, {
        line = 1,
        message = "JSON is not an array - queries may not work as expected",
        severity = "warning",
        error_type = "schema",
      })
    elseif result.valid and #decode_result == 0 then
      table.insert(result.warnings, {
        line = 1,
        message = "JSON array is empty",
        severity = "warning",
        error_type = "schema",
      })
    end
  end

  result.info = string.format(
    "JSON with %d errors, %d warnings",
    #result.errors,
    #result.warnings
  )

  return result
end

---Validate JSONL content
---@param content string JSONL content
---@param bufnr number Buffer number
---@return ValidationResult result
local function validate_jsonl(content, bufnr)
  local result = {
    valid = true,
    errors = {},
    warnings = {},
  }

  local lines = vim.split(content, '\n', { plain = true })
  local valid_lines = 0

  -- Validate each line
  for i, line in ipairs(lines) do
    if line:match('%S') then  -- Skip empty lines
      local ok, decode_result = pcall(vim.json.decode, line)

      if not ok then
        result.valid = false
        table.insert(result.errors, {
          line = i,
          message = string.format("Invalid JSON on line %d: %s", i, tostring(decode_result)),
          severity = "error",
          error_type = "json",
        })
      else
        valid_lines = valid_lines + 1

        -- Check if it's an object (expected for JSONL)
        if type(decode_result) ~= 'table' or #decode_result > 0 then
          table.insert(result.warnings, {
            line = i,
            message = "Expected JSON object, found " .. type(decode_result),
            severity = "warning",
            error_type = "schema",
          })
        end
      end
    end
  end

  if valid_lines == 0 then
    table.insert(result.warnings, {
      line = 1,
      message = "No valid JSON lines found",
      severity = "warning",
      error_type = "schema",
    })
  end

  -- Also validate with DuckDB
  if result.valid then
    local success, error_msg = try_parse_with_duckdb(content, 'jsonl')

    if not success and error_msg then
      result.valid = false
      local parsed_errors = parse_duckdb_error(error_msg, 'jsonl')
      for _, err in ipairs(parsed_errors) do
        table.insert(result.errors, err)
      end
    end
  end

  result.info = string.format(
    "JSONL with %d lines, %d valid, %d errors, %d warnings",
    #lines,
    valid_lines,
    #result.errors,
    #result.warnings
  )

  return result
end

---Validate buffer content
---@param identifier string|number|nil Buffer identifier
---@return ValidationResult? result
---@return string? error
function M.validate_buffer(identifier)
  local buffer_info, err = buffer_module.get_buffer_info(identifier)
  if not buffer_info then
    return nil, err
  end

  local result

  if buffer_info.format == 'csv' then
    result = validate_csv(buffer_info.content, buffer_info.bufnr)
  elseif buffer_info.format == 'json' then
    result = validate_json(buffer_info.content, buffer_info.bufnr)
  elseif buffer_info.format == 'jsonl' then
    result = validate_jsonl(buffer_info.content, buffer_info.bufnr)
  else
    return nil, string.format("Unsupported format for validation: %s", buffer_info.format)
  end

  return result
end

---Set Neovim diagnostics for validation errors
---@param bufnr number Buffer number
---@param validation_result ValidationResult Validation result
function M.set_diagnostics(bufnr, validation_result)
  local diagnostics = {}

  -- Add errors
  for _, err in ipairs(validation_result.errors) do
    table.insert(diagnostics, {
      bufnr = bufnr,
      lnum = (err.line or 1) - 1,  -- 0-indexed
      col = (err.column or 1) - 1,  -- 0-indexed
      severity = vim.diagnostic.severity.ERROR,
      source = 'duckdb',
      message = err.message,
      user_data = { error_type = err.error_type },
    })
  end

  -- Add warnings
  for _, warn in ipairs(validation_result.warnings) do
    table.insert(diagnostics, {
      bufnr = bufnr,
      lnum = (warn.line or 1) - 1,  -- 0-indexed
      col = (warn.column or 1) - 1,  -- 0-indexed
      severity = vim.diagnostic.severity.WARN,
      source = 'duckdb',
      message = warn.message,
      user_data = { error_type = warn.error_type },
    })
  end

  -- Set diagnostics
  local namespace = vim.api.nvim_create_namespace('duckdb_validation')
  vim.diagnostic.set(namespace, bufnr, diagnostics, {})
end

---Clear diagnostics for a buffer
---@param bufnr number Buffer number
function M.clear_diagnostics(bufnr)
  local namespace = vim.api.nvim_create_namespace('duckdb_validation')
  vim.diagnostic.reset(namespace, bufnr)
end

---Display validation results in a floating window
---@param validation_result ValidationResult Validation result
---@param buffer_name string Buffer name for display
function M.display_validation_results(validation_result, buffer_name)
  local lines = {}

  -- Title
  table.insert(lines, string.format("Validation Results: %s", buffer_name))
  table.insert(lines, string.rep("═", 60))
  table.insert(lines, "")

  -- Summary
  if validation_result.valid and #validation_result.errors == 0 and #validation_result.warnings == 0 then
    table.insert(lines, "✓ Valid! No errors or warnings found.")
  else
    if #validation_result.errors > 0 then
      table.insert(lines, string.format("✗ Errors: %d", #validation_result.errors))
    end
    if #validation_result.warnings > 0 then
      table.insert(lines, string.format("⚠ Warnings: %d", #validation_result.warnings))
    end
  end

  if validation_result.info then
    table.insert(lines, "")
    table.insert(lines, validation_result.info)
  end

  -- Errors
  if #validation_result.errors > 0 then
    table.insert(lines, "")
    table.insert(lines, "Errors:")
    table.insert(lines, string.rep("─", 60))

    for i, err in ipairs(validation_result.errors) do
      if i > 20 then  -- Limit display
        table.insert(lines, string.format("... and %d more errors", #validation_result.errors - 20))
        break
      end

      local location = ""
      if err.line then
        location = string.format("Line %d", err.line)
        if err.column then
          location = location .. string.format(", Col %d", err.column)
        end
        location = location .. ": "
      end

      table.insert(lines, string.format("%d. [%s] %s%s", i, err.error_type, location, err.message))
    end
  end

  -- Warnings
  if #validation_result.warnings > 0 then
    table.insert(lines, "")
    table.insert(lines, "Warnings:")
    table.insert(lines, string.rep("─", 60))

    for i, warn in ipairs(validation_result.warnings) do
      if i > 20 then  -- Limit display
        table.insert(lines, string.format("... and %d more warnings", #validation_result.warnings - 20))
        break
      end

      local location = ""
      if warn.line then
        location = string.format("Line %d", warn.line)
        if warn.column then
          location = location .. string.format(", Col %d", warn.column)
        end
        location = location .. ": "
      end

      table.insert(lines, string.format("%d. [%s] %s%s", i, warn.error_type, location, warn.message))
    end
  end

  table.insert(lines, "")
  table.insert(lines, "Press 'q' or <Esc> to close")

  -- Create buffer
  local buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_buf_set_option(buf, 'modifiable', false)
  vim.api.nvim_buf_set_option(buf, 'bufhidden', 'wipe')
  vim.api.nvim_buf_set_option(buf, 'filetype', 'duckdb-validation')

  -- Calculate window size
  local width = math.min(80, vim.o.columns - 4)
  local height = math.min(#lines + 2, vim.o.lines - 4)

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
    title = ' DuckDB Validation ',
    title_pos = 'center',
  }

  -- Create window
  local win = vim.api.nvim_open_win(buf, true, win_opts)

  -- Set window options
  vim.api.nvim_win_set_option(win, 'wrap', true)
  vim.api.nvim_win_set_option(win, 'cursorline', true)

  -- Add syntax highlighting for validation results
  vim.cmd([[
    syntax match DuckDBValidationError /^.*✗.*$/
    syntax match DuckDBValidationWarning /^.*⚠.*$/
    syntax match DuckDBValidationSuccess /^.*✓.*$/
    syntax match DuckDBValidationLocation /Line \d\+\(, Col \d\+\)\?:/
    syntax match DuckDBValidationType /\[.*\]/

    highlight DuckDBValidationError guifg=#ff6b6b ctermfg=203
    highlight DuckDBValidationWarning guifg=#ffd93d ctermfg=221
    highlight DuckDBValidationSuccess guifg=#6bcf7f ctermfg=114
    highlight DuckDBValidationLocation guifg=#74c0fc ctermfg=117
    highlight DuckDBValidationType guifg=#a78bfa ctermfg=141
  ]])

  -- Key mappings
  local keymaps = {
    { 'n', 'q', '<cmd>close<cr>', { buffer = buf, nowait = true, silent = true } },
    { 'n', '<Esc>', '<cmd>close<cr>', { buffer = buf, nowait = true, silent = true } },
  }

  for _, keymap in ipairs(keymaps) do
    vim.keymap.set(keymap[1], keymap[2], keymap[3], keymap[4])
  end
end

return M
