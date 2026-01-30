---@class DuckDBBuffer
local M = {}

---@class BufferInfo
---@field bufnr number Buffer number
---@field name string Buffer name
---@field filetype string Buffer filetype
---@field content string Buffer content
---@field format string Detected format (csv, json, jsonl)

---Parse buffer identifier (number, name, or current)
---@param identifier string|number|nil Buffer identifier
---@return number? bufnr
---@return string? error
local function parse_buffer_identifier(identifier)
  if not identifier then
    return vim.api.nvim_get_current_buf()
  end

  if type(identifier) == "number" then
    if vim.api.nvim_buf_is_valid(identifier) then
      return identifier
    end
    return nil, string.format("Buffer %d is not valid", identifier)
  end

  -- Try to find buffer by name
  local bufnr = vim.fn.bufnr(identifier)
  if bufnr ~= -1 then
    return bufnr
  end

  -- Try parsing as number
  local num = tonumber(identifier)
  if num and vim.api.nvim_buf_is_valid(num) then
    return num
  end

  return nil, string.format("Buffer '%s' not found", identifier)
end

---Detect file format from filetype or filename
---@param filetype string
---@param filename string
---@return string format
local function detect_format(filetype, filename)
  -- Check filetype first
  if filetype == "csv" then
    return "csv"
  elseif filetype == "json" then
    return "json"
  elseif filetype == "jsonl" or filetype == "ndjson" then
    return "jsonl"
  end

  -- Check filename extension
  if filename:match("%.csv$") then
    return "csv"
  elseif filename:match("%.jsonl$") or filename:match("%.ndjson$") then
    return "jsonl"
  elseif filename:match("%.json$") then
    return "json"
  end

  -- Default to csv if uncertain
  return "csv"
end

---Get buffer information and content
---@param identifier string|number|nil Buffer identifier
---@return BufferInfo? info
---@return string? error
function M.get_buffer_info(identifier)
  local bufnr, err = parse_buffer_identifier(identifier)
  if not bufnr then
    return nil, err
  end

  local name = vim.api.nvim_buf_get_name(bufnr)
  local filetype = vim.api.nvim_get_option_value("filetype", { buf = bufnr })
  -- vim.api.nvim_buf_get_option(bufnr, 'filetype')

  -- Get buffer content
  local lines = vim.api.nvim_buf_get_lines(bufnr, 0, -1, false)
  if #lines == 0 then
    return nil, string.format("Buffer %d is empty", bufnr)
  end

  local content = table.concat(lines, "\n")
  local format = detect_format(filetype, name)

  -- Check content size
  local content_size = #content

  -- Warn for very large buffers (>100MB)
  if content_size > 100 * 1024 * 1024 then
    vim.notify(
      string.format(
        "[DuckDB] Warning: Buffer is %dMB. Query may be slow.",
        math.floor(content_size / 1024 / 1024)
      ),
      vim.log.levels.WARN
    )
  end

  -- Hard limit to prevent crashes (500MB)
  if content_size > 500 * 1024 * 1024 then
    return nil,
      string.format(
        "Buffer too large (%dMB). Maximum is 500MB.",
        math.floor(content_size / 1024 / 1024)
      )
  end

  return {
    bufnr = bufnr,
    name = name ~= "" and name or string.format("buffer_%d", bufnr),
    filetype = filetype,
    content = content,
    format = format,
  }
end

---Get multiple buffer infos
---@param identifiers table<string|number> List of buffer identifiers
---@return table<string, BufferInfo> buffers Map of table name to buffer info
---@return string? error
function M.get_multiple_buffers(identifiers)
  local buffers = {}

  for _, id in ipairs(identifiers) do
    local info, err = M.get_buffer_info(id)
    if not info then
      return nil, err
    end

    -- Generate table name from buffer
    local table_name
    if type(id) == "string" then
      -- Use the identifier as table name, sanitized
      table_name = id:gsub("[^%w_]", "_")
    else
      -- Use buffer name or number
      local basename = vim.fn.fnamemodify(info.name, ":t:r")
      table_name = basename ~= "" and basename or string.format("buffer_%d", info.bufnr)
    end

    buffers[table_name] = info
  end

  return buffers
end

---Extract table name from query
---@param query string SQL query
---@return table<string> identifiers List of buffer identifiers found
function M.extract_buffer_references(query)
  local identifiers = {}
  local seen = {}

  -- Match buffer() function calls
  -- Patterns: buffer('name'), buffer("name"), buffer(number)
  for match in query:gmatch("buffer%s*%(%s*['\"]([^'\"]+)['\"]%s*%)") do
    if not seen[match] then
      table.insert(identifiers, match)
      seen[match] = true
    end
  end

  for match in query:gmatch("buffer%s*%(%s*(%d+)%s*%)") do
    local num = tonumber(match)
    if num and not seen[num] then
      table.insert(identifiers, num)
      seen[num] = true
    end
  end

  -- Check if query references 'buffer' table without function call
  if query:match("%f[%w]buffer%f[%W]") and #identifiers == 0 then
    -- Default to current buffer
    table.insert(identifiers, vim.api.nvim_get_current_buf())
  end

  return identifiers
end

---Validate CSV content
---@param content string
---@return boolean valid
---@return string? error
function M.validate_csv(content)
  local lines = vim.split(content, "\n", { plain = true })
  if #lines < 1 then
    return false, "CSV must have at least one line"
  end
  return true
end

---Validate JSON content
---@param content string
---@return boolean valid
---@return string? error
function M.validate_json(content)
  local ok, _ = pcall(vim.json.decode, content)
  if not ok then
    return false, "Invalid JSON content"
  end
  return true
end

---Validate JSONL content
---@param content string
---@return boolean valid
---@return string? error
function M.validate_jsonl(content)
  local lines = vim.split(content, "\n", { plain = true })
  for i, line in ipairs(lines) do
    if line:match("%S") then -- Skip empty lines
      local ok, _ = pcall(vim.json.decode, line)
      if not ok then
        return false, string.format("Invalid JSON on line %d", i)
      end
    end
  end
  return true
end

---Validate buffer content based on format
---@param content string
---@param format string
---@return boolean valid
---@return string? error
function M.validate_content(content, format)
  if format == "csv" then
    return M.validate_csv(content)
  elseif format == "json" then
    return M.validate_json(content)
  elseif format == "jsonl" then
    return M.validate_jsonl(content)
  end
  return true
end

return M
