---@class DuckDBValidate
---Validation module that leverages DuckDB's excellent error reporting
---Uses reject_errors and reject_scans tables for detailed CSV validation
local M = {}

local query_module = require("duckdb.query")
local buffer_module = require("duckdb.buffer")

---@class ValidationError
---@field line number|nil Line number where error occurred (1-based)
---@field column number|nil Column number where error occurred (1-based)
---@field message string Error message
---@field severity string "error" | "warning" | "info"
---@field error_type string Type of error (e.g., "CAST", "MISSING COLUMNS", "schema")
---@field csv_line string|nil The actual CSV line content (for CSV errors)

---@class ValidationResult
---@field valid boolean Whether the buffer is valid
---@field errors table<ValidationError> List of validation errors
---@field warnings table<ValidationError> List of warnings
---@field info string|nil Additional information

---@class CSVRejectError
---@field line number Line number in source file (1-based)
---@field column_idx number Column index (0-based)
---@field column_name string|nil Column name
---@field error_type string Type of error (CAST, MISSING COLUMNS, etc.)
---@field csv_line string The actual CSV line content
---@field error_message string Detailed error message

-- ============================================================================
-- CSV Column Position Helper
-- ============================================================================

---Find character position of nth CSV column in a line (handles quoted fields)
---@param line string CSV line content
---@param column_num number 1-based column number
---@return number col 0-based character position
local function find_csv_column_position(line, column_num)
  if column_num <= 1 then
    return 0
  end

  local col_count = 1
  local in_quotes = false

  for i = 1, #line do
    local char = line:sub(i, i)
    if char == '"' then
      in_quotes = not in_quotes
    elseif char == ',' and not in_quotes then
      col_count = col_count + 1
      if col_count == column_num then
        return i -- Position after the comma
      end
    end
  end

  return 0
end

-- ============================================================================
-- CSV Validation with reject_errors/reject_scans
-- ============================================================================

---Try to parse CSV content with DuckDB and capture reject errors
---@param file_path string Path to CSV file
---@return boolean success
---@return CSVRejectError[]|nil errors Array of reject errors (nil on connection failure)
---@return string|nil scan_info Information about the scan
local function try_parse_csv_with_rejects(file_path)
  local conn, err = query_module.create_connection()
  if not conn then
    return false, nil, "Failed to create DuckDB connection: " .. (err or "unknown error")
  end

  local errors = {}
  local scan_info = nil
  local success = true

  local ok, parse_err = pcall(function()
    -- Read CSV with store_rejects enabled and ignore_errors to continue past errors
    local escaped_path = file_path:gsub("'", "''")
    local read_query = string.format([[
      SELECT * FROM read_csv('%s',
        sample_size=-1,
        store_rejects=true,
        ignore_errors=true
      )
    ]], escaped_path)

    -- Execute the read (this populates reject tables even if it succeeds)
    local _ = query_module.execute_query(conn, read_query)

    -- Query errors are expected if CSV is malformed - we continue to check reject tables

    -- Query reject_errors table for detailed error information
    local reject_result = query_module.execute_query(conn, [[
      SELECT
        line,
        column_idx,
        column_name,
        error_type,
        csv_line,
        error_message
      FROM reject_errors
      ORDER BY line, column_idx
    ]])

    if reject_result and reject_result.row_count > 0 then
      success = false
      for _, row in ipairs(reject_result.rows) do
        table.insert(errors, {
          line = tonumber(row[1]) or 1,
          column_idx = tonumber(row[2]) or 0,
          column_name = row[3] or "unknown",
          error_type = row[4] or "unknown",
          csv_line = row[5] or "",
          error_message = row[6] or "Unknown error",
        })
      end
    end

    -- Query reject_scans for scan metadata
    local scan_result = query_module.execute_query(conn, [[
      SELECT file_path, delimiter FROM reject_scans LIMIT 1
    ]])

    if scan_result and scan_result.row_count > 0 then
      local row = scan_result.rows[1]
      scan_info = string.format("File: %s, Delimiter: '%s'", row[1] or "?", row[2] or ",")
    end
  end)

  query_module.close_connection(conn)

  if not ok then
    return false, nil, tostring(parse_err)
  end

  return success or #errors == 0, errors, scan_info
end

---Validate CSV content and provide detailed error information using reject tables
---@param file_path string Path to CSV file
---@return ValidationResult result
local function validate_csv(file_path)
  local result = {
    valid = true,
    errors = {},
    warnings = {},
    info = nil,
  }

  local success, reject_errors, scan_info = try_parse_csv_with_rejects(file_path)

  if reject_errors and #reject_errors > 0 then
    result.valid = false

    for _, err in ipairs(reject_errors) do
      table.insert(result.errors, {
        line = err.line,
        column = err.column_idx + 1,  -- Convert 0-based to 1-based
        message = string.format("[%s] %s (column: %s)",
          err.error_type,
          err.error_message,
          err.column_name
        ),
        severity = "error",
        error_type = err.error_type,
        csv_line = err.csv_line,
      })
    end
  elseif not success then
    -- Connection or parsing failed entirely
    table.insert(result.errors, {
      line = 1,
      message = "Failed to parse CSV file",
      severity = "error",
      error_type = "parse",
    })
    result.valid = false
  end

  -- Build info string
  local error_count = #result.errors
  if scan_info then
    result.info = scan_info
  end

  if error_count > 0 then
    result.info = (result.info or "") .. string.format(" | %d validation error(s) found", error_count)
  else
    result.info = (result.info or "") .. " | No errors found"
  end

  return result
end

-- ============================================================================
-- JSON Validation
-- ============================================================================

---Validate JSON content
---@param content string JSON content
---@return ValidationResult result
local function validate_json(content)
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
    -- Check if it's an array (expected for querying)
    if type(decode_result) ~= "table" then
      table.insert(result.warnings, {
        line = 1,
        message = "JSON is not an array - queries may not work as expected",
        severity = "warning",
        error_type = "schema",
      })
    elseif vim.tbl_islist(decode_result) and #decode_result == 0 then
      table.insert(result.warnings, {
        line = 1,
        message = "JSON array is empty",
        severity = "warning",
        error_type = "schema",
      })
    end
  end

  result.info = string.format("JSON with %d errors, %d warnings", #result.errors, #result.warnings)

  return result
end

---Validate JSONL content (newline-delimited JSON)
---@param content string JSONL content
---@return ValidationResult result
local function validate_jsonl(content)
  local result = {
    valid = true,
    errors = {},
    warnings = {},
  }

  local lines = vim.split(content, "\n", { plain = true })
  local valid_lines = 0

  -- Validate each line
  for i, line in ipairs(lines) do
    if line:match("%S") then -- Skip empty lines
      local ok, decode_result = pcall(vim.json.decode, line)

      if not ok then
        result.valid = false
        table.insert(result.errors, {
          line = i,
          message = string.format("Invalid JSON: %s", tostring(decode_result)),
          severity = "error",
          error_type = "json",
        })
      else
        valid_lines = valid_lines + 1

        -- Check if it's an object (expected for JSONL)
        if type(decode_result) ~= "table" or vim.tbl_islist(decode_result) then
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

  result.info = string.format(
    "JSONL with %d lines, %d valid, %d errors, %d warnings",
    #lines,
    valid_lines,
    #result.errors,
    #result.warnings
  )

  return result
end

-- ============================================================================
-- Public API
-- ============================================================================

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
  local temp_file_to_cleanup = nil

  if buffer_info.format == "csv" then
    -- For CSV, we need an actual file path for DuckDB's reject tables
    -- If buffer has a file, use it; otherwise write to temp file
    local file_path = buffer_info.name
    if file_path == "" or not vim.fn.filereadable(file_path) then
      -- Write buffer content to temp file using vim.fn.tempname()
      file_path = vim.fn.tempname() .. ".csv"
      local file, open_err = io.open(file_path, "w")
      if not file then
        return nil, "Failed to create temporary file for validation: " .. (open_err or "unknown error")
      end

      local success, write_err = pcall(file.write, file, buffer_info.content)
      file:close()

      if not success then
        pcall(os.remove, file_path)
        return nil, "Failed to write temporary file for validation: " .. (write_err or "unknown error")
      end

      -- Track for cleanup after validation
      temp_file_to_cleanup = file_path
    end

    result = validate_csv(file_path)

    -- Clean up temp file if we created one
    if temp_file_to_cleanup then
      pcall(os.remove, temp_file_to_cleanup)
    end
  elseif buffer_info.format == "json" then
    result = validate_json(buffer_info.content)
  elseif buffer_info.format == "jsonl" then
    result = validate_jsonl(buffer_info.content)
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
  local namespace = vim.api.nvim_create_namespace("duckdb_validation")

  -- Clear existing diagnostics and autocmds first
  vim.diagnostic.reset(namespace, bufnr)
  pcall(vim.api.nvim_del_augroup_by_name, "duckdb_validation_" .. bufnr)

  local line_count = vim.api.nvim_buf_line_count(bufnr)

  -- Add errors with line mapping from reject_errors
  for _, err in ipairs(validation_result.errors) do
    -- Line numbers from reject_errors are 1-based file lines
    -- Neovim diagnostics use 0-based indexing
    local lnum = (err.line or 1) - 1

    -- Ensure line number is valid for buffer
    if lnum >= line_count then
      lnum = line_count - 1
    end
    if lnum < 0 then
      lnum = 0
    end

    -- Column from reject_errors is 1-based column index
    -- Map to character position if possible
    local col = 0
    if err.column and err.column > 0 then
      -- Try to find the column position in the actual line
      local line_content = vim.api.nvim_buf_get_lines(bufnr, lnum, lnum + 1, false)[1]
      if line_content then
        col = find_csv_column_position(line_content, err.column)
      end
    end

    table.insert(diagnostics, {
      bufnr = bufnr,
      lnum = lnum,
      col = col,
      severity = vim.diagnostic.severity.ERROR,
      source = "duckdb",
      message = M.sanitize(err.message),
      user_data = {
        error_type = err.error_type,
        csv_line = err.csv_line,
      },
    })
  end

  -- Add warnings
  for _, warn in ipairs(validation_result.warnings) do
    local lnum = (warn.line or 1) - 1
    if lnum >= line_count then
      lnum = line_count - 1
    end
    if lnum < 0 then
      lnum = 0
    end

    table.insert(diagnostics, {
      bufnr = bufnr,
      lnum = lnum,
      col = (warn.column or 1) - 1,
      severity = vim.diagnostic.severity.WARN,
      source = "duckdb",
      message = M.sanitize(warn.message),
      user_data = { error_type = warn.error_type },
    })
  end

  vim.diagnostic.set(namespace, bufnr, diagnostics, {})

  -- Auto-clear diagnostics when buffer is modified (idiomatic Neovim behavior)
  local augroup = vim.api.nvim_create_augroup("duckdb_validation_" .. bufnr, { clear = true })
  vim.api.nvim_create_autocmd({ "TextChanged", "TextChangedI" }, {
    group = augroup,
    buffer = bufnr,
    once = true,
    callback = function()
      M.clear_diagnostics(bufnr)
    end,
  })
end

---Clear diagnostics for a buffer
---@param bufnr number Buffer number
function M.clear_diagnostics(bufnr)
  local namespace = vim.api.nvim_create_namespace("duckdb_validation")
  vim.diagnostic.reset(namespace, bufnr)
  -- Clean up autocmd group if it exists
  pcall(vim.api.nvim_del_augroup_by_name, "duckdb_validation_" .. bufnr)
end

---Sanitize string for display (remove newlines)
---@param str string Input string
---@return string Sanitized string
function M.sanitize(str)
  if not str then return "" end
  return str:gsub("\n", " "):gsub("\r", "")
end

---Display validation results in a floating window with jump-to-error support
---@param validation_result ValidationResult Validation result
---@param buffer_name string Buffer name for display
function M.display_validation_results(validation_result, buffer_name)
  local lines = {}

  -- Title
  table.insert(lines, string.format("Validation Results: %s", vim.fn.fnamemodify(buffer_name, ":t")))
  table.insert(lines, string.rep("=", 70))
  table.insert(lines, "")

  -- Summary
  if validation_result.valid and #validation_result.errors == 0 and #validation_result.warnings == 0 then
    table.insert(lines, "Status: VALID - No errors found")
  else
    table.insert(lines, string.format("Status: INVALID - %d error(s), %d warning(s)",
      #validation_result.errors, #validation_result.warnings))
  end

  if validation_result.info then
    table.insert(lines, "")
    table.insert(lines, validation_result.info)
  end

  -- Detailed Errors
  if #validation_result.errors > 0 then
    table.insert(lines, "")
    table.insert(lines, "Errors:")
    table.insert(lines, string.rep("-", 70))

    for i, err in ipairs(validation_result.errors) do
      if i > 50 then  -- Limit display
        table.insert(lines, string.format("... and %d more errors", #validation_result.errors - 50))
        break
      end

      -- Error header with line number
      local location = ""
      if err.line then
        location = string.format("Line %d", err.line)
        if err.column then
          location = location .. string.format(", Col %d", err.column)
        end
      end

      table.insert(lines, "")
      table.insert(lines, string.format("%d. %s", i, location))
      table.insert(lines, string.format("   Type: %s", err.error_type or "unknown"))
      table.insert(lines, string.format("   Message: %s", M.sanitize(err.message)))

      -- Show the problematic CSV line if available
      if err.csv_line and err.csv_line ~= "" then
        local display_line = err.csv_line
        if #display_line > 60 then
          display_line = display_line:sub(1, 57) .. "..."
        end
        table.insert(lines, string.format("   Content: %s", display_line))
      end
    end
  end

  -- Warnings section
  if #validation_result.warnings > 0 then
    table.insert(lines, "")
    table.insert(lines, "Warnings:")
    table.insert(lines, string.rep("-", 70))

    for i, warn in ipairs(validation_result.warnings) do
      if i > 20 then
        table.insert(lines, string.format("... and %d more warnings", #validation_result.warnings - 20))
        break
      end

      local location = warn.line and string.format("Line %d: ", warn.line) or ""
      table.insert(lines, string.format("%d. %s%s", i, location, warn.message))
    end
  end

  table.insert(lines, "")
  table.insert(lines, "Press 'q' or <Esc> to close | 'g' to go to first error | <CR> to jump")

  -- Create buffer
  local buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_set_option_value("modifiable", false, { buf = buf })
  vim.api.nvim_set_option_value("bufhidden", "wipe", { buf = buf })
  vim.api.nvim_set_option_value("filetype", "duckdb-validation", { buf = buf })

  -- Calculate window size
  local width = math.min(80, vim.o.columns - 4)
  local height = math.min(#lines + 2, vim.o.lines - 4)

  -- Calculate position (centered)
  local row = math.floor((vim.o.lines - height) / 2)
  local col = math.floor((vim.o.columns - width) / 2)

  -- Window options
  local win_opts = {
    relative = "editor",
    width = width,
    height = height,
    row = row,
    col = col,
    style = "minimal",
    border = "rounded",
    title = " DuckDB Validation ",
    title_pos = "center",
  }

  -- Create window
  local win = vim.api.nvim_open_win(buf, true, win_opts)

  -- Set window options
  vim.api.nvim_set_option_value("wrap", true, { win = win })
  vim.api.nvim_set_option_value("cursorline", true, { win = win })

  -- Add syntax highlighting for validation results
  vim.cmd([[
    syntax match DuckDBValidationError /Status: INVALID/
    syntax match DuckDBValidationSuccess /Status: VALID/
    syntax match DuckDBValidationLocation /Line \d\+\(, Col \d\+\)\?/
    syntax match DuckDBValidationType /Type: \w\+/
    syntax match DuckDBValidationNumber /^\s*\d\+\./

    highlight DuckDBValidationError guifg=#ff6b6b ctermfg=203
    highlight DuckDBValidationSuccess guifg=#6bcf7f ctermfg=114
    highlight DuckDBValidationLocation guifg=#74c0fc ctermfg=117
    highlight DuckDBValidationType guifg=#a78bfa ctermfg=141
    highlight DuckDBValidationNumber guifg=#ffd93d ctermfg=221
  ]])

  -- Store original window to return to
  local orig_win = vim.fn.win_getid(vim.fn.winnr('#'))

  -- Key mappings
  vim.keymap.set("n", "q", "<cmd>close<cr>", { buffer = buf, nowait = true, silent = true })
  vim.keymap.set("n", "<Esc>", "<cmd>close<cr>", { buffer = buf, nowait = true, silent = true })

  -- Jump to first error
  local first_error = validation_result.errors[1]
  if first_error and first_error.line then
    vim.keymap.set("n", "g", function()
      vim.cmd("close")
      if vim.api.nvim_win_is_valid(orig_win) then
        vim.api.nvim_set_current_win(orig_win)
      end
      vim.api.nvim_win_set_cursor(0, { first_error.line, (first_error.column or 1) - 1 })
    end, { buffer = buf, nowait = true, silent = true, desc = "Go to first error" })
  end

  -- Jump to error under cursor (parse error number from line)
  vim.keymap.set("n", "<CR>", function()
    local cursor_line = vim.api.nvim_win_get_cursor(win)[1]

    -- Search backwards from cursor for an error number
    for l = cursor_line, 1, -1 do
      local content = vim.api.nvim_buf_get_lines(buf, l - 1, l, false)[1]
      local err_num = content:match("^%s*(%d+)%.")
      if err_num then
        local err = validation_result.errors[tonumber(err_num)]
        if err and err.line then
          vim.cmd("close")
          if vim.api.nvim_win_is_valid(orig_win) then
            vim.api.nvim_set_current_win(orig_win)
          end
          vim.api.nvim_win_set_cursor(0, { err.line, (err.column or 1) - 1 })
          return
        end
      end
    end
    -- No error found under cursor
    vim.notify("No error found at cursor position", vim.log.levels.INFO)
  end, { buffer = buf, nowait = true, silent = true, desc = "Go to error under cursor" })

  -- Auto-close floating window when leaving buffer
  vim.api.nvim_create_autocmd("BufLeave", {
    buffer = buf,
    once = true,
    callback = function()
      if vim.api.nvim_win_is_valid(win) then
        vim.api.nvim_win_close(win, true)
      end
    end,
  })
end

return M
