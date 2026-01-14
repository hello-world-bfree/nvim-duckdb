---@class DuckDBQuery
local M = {}

local ffi = require("ffi")
local duckdb_ffi = require("duckdb.ffi")
local buffer_module = require("duckdb.buffer")

-- Type constants for cleaner code
local T = duckdb_ffi.types

---@class DuckDBConnection
---@field db ffi.cdata* Database handle
---@field conn ffi.cdata* Connection handle
---@field temp_dir string? Temporary directory for data files
---@field temp_files table<string> List of temporary files to clean up
---@field _closed boolean Whether the connection has been closed

---@class QueryResult
---@field columns table<string> Column names
---@field rows table<table> Row data
---@field row_count number Number of rows
---@field column_count number Number of columns
---@field rows_changed number Number of rows affected (for DML)

-- ============================================================================
-- Type Formatting Helpers
-- ============================================================================

---Format timestamp (microseconds since epoch) to ISO string
---@param micros number Microseconds since epoch
---@return string
local function format_timestamp(micros)
  local seconds = math.floor(micros / 1000000)
  local us = micros % 1000000
  if us < 0 then
    us = us + 1000000
    seconds = seconds - 1
  end
  return os.date("!%Y-%m-%d %H:%M:%S", seconds) .. string.format(".%06d", us)
end

---Format date (days since epoch) to ISO string
---@param days number Days since 1970-01-01
---@return string
local function format_date(days)
  local seconds = days * 86400
  return os.date("!%Y-%m-%d", seconds)
end

---Format time (microseconds since midnight) to string
---@param micros number Microseconds since midnight
---@return string
local function format_time(micros)
  local total_seconds = math.floor(micros / 1000000)
  local us = micros % 1000000
  local hours = math.floor(total_seconds / 3600)
  local minutes = math.floor((total_seconds % 3600) / 60)
  local seconds = total_seconds % 60
  return string.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, us)
end

---Format hugeint (128-bit signed integer) to string
---@param hugeint ffi.cdata* duckdb_hugeint structure
---@return string
local function format_hugeint(hugeint)
  local upper = tonumber(hugeint.upper)
  local lower = tonumber(hugeint.lower)

  if upper == 0 then
    return tostring(lower)
  elseif upper == -1 and lower >= 0x8000000000000000ULL then
    -- Small negative number
    return tostring(lower - 0x10000000000000000)
  else
    -- Large number - show approximation
    return string.format("%d*2^64+%u", upper, lower)
  end
end

---Format interval to string
---@param interval ffi.cdata* duckdb_interval structure
---@return string
local function format_interval(interval)
  local parts = {}
  if interval.months ~= 0 then
    table.insert(parts, string.format("%d months", interval.months))
  end
  if interval.days ~= 0 then
    table.insert(parts, string.format("%d days", interval.days))
  end
  if interval.micros ~= 0 then
    local total_seconds = math.floor(interval.micros / 1000000)
    local hours = math.floor(total_seconds / 3600)
    local minutes = math.floor((total_seconds % 3600) / 60)
    local seconds = total_seconds % 60
    table.insert(parts, string.format("%02d:%02d:%02d", hours, minutes, seconds))
  end
  return #parts > 0 and table.concat(parts, " ") or "0"
end

-- ============================================================================
-- Vector Value Extraction (Modern API)
-- ============================================================================

---Extract a single value from a vector at a given row index
---@param vector ffi.cdata* Vector handle
---@param col_type number Column type enum value
---@param row number Row index within chunk (0-based)
---@param validity ffi.cdata*? Validity mask (nil if no NULLs in column)
---@return any value
local function extract_vector_value(vector, col_type, row, validity)
  -- Check NULL via validity mask
  if validity ~= nil and not duckdb_ffi.C.duckdb_validity_row_is_valid(validity, row) then
    return nil
  end

  local data = duckdb_ffi.C.duckdb_vector_get_data(vector)
  if data == nil then
    return nil
  end

  -- Type-specific extraction using ffi.cast
  if col_type == T.BOOLEAN then
    local bool_data = ffi.cast("bool*", data)
    return bool_data[row]
  elseif col_type == T.TINYINT then
    local int8_data = ffi.cast("int8_t*", data)
    return tonumber(int8_data[row])
  elseif col_type == T.SMALLINT then
    local int16_data = ffi.cast("int16_t*", data)
    return tonumber(int16_data[row])
  elseif col_type == T.INTEGER then
    local int32_data = ffi.cast("int32_t*", data)
    return tonumber(int32_data[row])
  elseif col_type == T.BIGINT then
    local int64_data = ffi.cast("int64_t*", data)
    return tonumber(int64_data[row])
  elseif col_type == T.UTINYINT then
    local uint8_data = ffi.cast("uint8_t*", data)
    return tonumber(uint8_data[row])
  elseif col_type == T.USMALLINT then
    local uint16_data = ffi.cast("uint16_t*", data)
    return tonumber(uint16_data[row])
  elseif col_type == T.UINTEGER then
    local uint32_data = ffi.cast("uint32_t*", data)
    return tonumber(uint32_data[row])
  elseif col_type == T.UBIGINT then
    local uint64_data = ffi.cast("uint64_t*", data)
    return tonumber(uint64_data[row])
  elseif col_type == T.FLOAT then
    local float_data = ffi.cast("float*", data)
    return tonumber(float_data[row])
  elseif col_type == T.DOUBLE then
    local double_data = ffi.cast("double*", data)
    return tonumber(double_data[row])
  elseif col_type == T.VARCHAR or col_type == T.BLOB then
    local str_data = ffi.cast("duckdb_string_t*", data)
    return duckdb_ffi.extract_string(str_data + row)
  elseif col_type == T.TIMESTAMP or col_type == T.TIMESTAMP_S or col_type == T.TIMESTAMP_MS or col_type == T.TIMESTAMP_NS or col_type == T.TIMESTAMP_TZ then
    local ts_data = ffi.cast("duckdb_timestamp*", data)
    return format_timestamp(tonumber(ts_data[row].micros))
  elseif col_type == T.DATE then
    local date_data = ffi.cast("duckdb_date*", data)
    return format_date(tonumber(date_data[row].days))
  elseif col_type == T.TIME or col_type == T.TIME_TZ then
    local time_data = ffi.cast("duckdb_time*", data)
    return format_time(tonumber(time_data[row].micros))
  elseif col_type == T.INTERVAL then
    local interval_data = ffi.cast("duckdb_interval*", data)
    return format_interval(interval_data + row)
  elseif col_type == T.HUGEINT then
    local hugeint_data = ffi.cast("duckdb_hugeint*", data)
    return format_hugeint(hugeint_data + row)
  elseif col_type == T.UHUGEINT then
    local uhugeint_data = ffi.cast("duckdb_uhugeint*", data)
    -- Simple representation for unsigned 128-bit
    local upper = tonumber(uhugeint_data[row].upper)
    local lower = tonumber(uhugeint_data[row].lower)
    if upper == 0 then
      return tostring(lower)
    else
      return string.format("%u*2^64+%u", upper, lower)
    end
  elseif col_type == T.UUID then
    -- UUID is stored as hugeint, format as UUID string
    local uuid_data = ffi.cast("duckdb_hugeint*", data)
    local upper = uuid_data[row].upper
    local lower = uuid_data[row].lower
    -- Format as UUID (simplified)
    return string.format("%016x%016x", tonumber(upper), tonumber(lower))
  else
    -- For unsupported types (LIST, STRUCT, MAP, ENUM, etc.), return type name
    return string.format("[%s]", duckdb_ffi.type_names[col_type] or "UNKNOWN")
  end
end

-- ============================================================================
-- Connection Management
-- ============================================================================

---Create a new DuckDB connection
---@return DuckDBConnection? connection
---@return string? error
function M.create_connection()
  if not duckdb_ffi.lib then
    return nil, "DuckDB library not loaded"
  end

  local db = ffi.new("duckdb_database[1]")
  local conn = ffi.new("duckdb_connection[1]")

  -- Open in-memory database
  local state = duckdb_ffi.C.duckdb_open(nil, db)
  if state ~= 0 then
    return nil, "Failed to open DuckDB database"
  end

  -- Create connection
  state = duckdb_ffi.C.duckdb_connect(db[0], conn)
  if state ~= 0 then
    duckdb_ffi.C.duckdb_close(db)
    return nil, "Failed to create DuckDB connection"
  end

  -- Create connection object with explicit lifecycle management
  -- Note: FFI finalizers are unreliable for C library cleanup, so we don't use them.
  -- Users MUST call close_connection() explicitly to avoid resource leaks.
  local connection = {
    db = db,
    conn = conn,
    temp_files = {},
    _closed = false,
  }

  return connection
end

---Close DuckDB connection and clean up temp files
---@param connection DuckDBConnection
function M.close_connection(connection)
  -- Prevent double-close which can cause crashes
  if connection._closed then
    return
  end
  connection._closed = true

  -- Clean up temporary files
  if connection.temp_files then
    for _, filepath in ipairs(connection.temp_files) do
      pcall(os.remove, filepath)
    end
    connection.temp_files = {}
  end

  -- Important: Disconnect connection BEFORE closing database
  -- Wrong order can cause use-after-free crashes
  if connection.conn then
    duckdb_ffi.C.duckdb_disconnect(connection.conn)
    connection.conn = nil
  end
  if connection.db then
    duckdb_ffi.C.duckdb_close(connection.db)
    connection.db = nil
  end
end

-- ============================================================================
-- Query Execution (Modern Data Chunk API)
-- ============================================================================

---Execute a query and return results using modern data chunk API
---@param connection DuckDBConnection
---@param query string SQL query
---@return QueryResult? result
---@return string? error
function M.execute_query(connection, query)
  if not connection.conn then
    return nil, "Connection is closed"
  end

  if connection._closed then
    return nil, "Connection is closed"
  end

  local result = ffi.new("duckdb_result[1]")

  local state = duckdb_ffi.C.duckdb_query(connection.conn[0], query, result)

  if state ~= 0 then
    local error_msg = "Query failed"
    local err_ptr = duckdb_ffi.C.duckdb_result_error(result)
    if err_ptr ~= nil then
      error_msg = ffi.string(err_ptr)
    end
    duckdb_ffi.C.duckdb_destroy_result(result)
    return nil, error_msg
  end

  -- Extract column information
  local column_count = tonumber(duckdb_ffi.C.duckdb_column_count(result))
  local rows_changed = tonumber(duckdb_ffi.C.duckdb_rows_changed(result))

  local columns = {}
  local column_types = {}
  for col = 0, column_count - 1 do
    local col_name = duckdb_ffi.C.duckdb_column_name(result, col)
    if col_name ~= nil then
      table.insert(columns, ffi.string(col_name))
    else
      table.insert(columns, string.format("column_%d", col))
    end
    table.insert(column_types, tonumber(duckdb_ffi.C.duckdb_column_type(result, col)))
  end

  -- Extract row data using data chunks (modern API)
  local rows = {}
  local total_row_count = 0

  -- Fetch chunks until exhausted
  while true do
    local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
    if chunk == nil then
      break
    end

    local chunk_size = tonumber(duckdb_ffi.C.duckdb_data_chunk_get_size(chunk))
    total_row_count = total_row_count + chunk_size

    -- Cache vectors and validity masks for this chunk
    local vectors = {}
    local validities = {}
    for col = 0, column_count - 1 do
      vectors[col] = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, col)
      validities[col] = duckdb_ffi.C.duckdb_vector_get_validity(vectors[col])
    end

    -- Extract rows from this chunk
    for row = 0, chunk_size - 1 do
      local row_data = {}
      for col = 0, column_count - 1 do
        local value = extract_vector_value(
          vectors[col],
          column_types[col + 1],
          row,
          validities[col]
        )
        table.insert(row_data, value)
      end
      table.insert(rows, row_data)
    end

    -- Destroy chunk after processing
    local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
    duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)
  end

  duckdb_ffi.C.duckdb_destroy_result(result)

  return {
    columns = columns,
    rows = rows,
    row_count = total_row_count,
    column_count = column_count,
    rows_changed = rows_changed,
  }
end

-- ============================================================================
-- Temp File Management
-- ============================================================================

---Write content to a temporary file with proper error handling
---@param content string Content to write
---@param extension string File extension (e.g., ".csv", ".json")
---@return string? filepath
---@return string? error
local function write_temp_file(content, extension)
  local temp_path = vim.fn.tempname() .. extension

  local file, err = io.open(temp_path, "w")
  if not file then
    return nil, "Failed to create temp file: " .. (err or "unknown error")
  end

  local success, write_err = pcall(file.write, file, content)
  file:close()

  if not success then
    pcall(os.remove, temp_path)
    return nil, "Failed to write temp file: " .. (write_err or "unknown error")
  end

  return temp_path
end

-- ============================================================================
-- Buffer Data Loading
-- ============================================================================

---Load buffer data into a table
---@param connection DuckDBConnection
---@param table_name string Table name
---@param buffer_info BufferInfo Buffer information
---@return boolean success
---@return string? error
function M.load_buffer_data(connection, table_name, buffer_info)
  local temp_path, file_err
  local create_query

  if buffer_info.format == "csv" then
    -- Write CSV to temp file using proper I/O
    temp_path, file_err = write_temp_file(buffer_info.content, ".csv")
    if not temp_path then
      return false, file_err
    end

    -- Track temp file for cleanup
    table.insert(connection.temp_files, temp_path)

    -- Use DuckDB's read_csv_auto to auto-detect schema
    create_query =
      string.format("CREATE TEMP TABLE %s AS SELECT * FROM read_csv('%s')", table_name, temp_path)
  elseif buffer_info.format == "json" then
    -- Write JSON to temp file
    temp_path, file_err = write_temp_file(buffer_info.content, ".json")
    if not temp_path then
      return false, file_err
    end

    -- Track temp file for cleanup
    table.insert(connection.temp_files, temp_path)

    create_query = string.format(
      "CREATE TEMP TABLE %s AS SELECT * FROM read_json('%s')",
      table_name,
      temp_path
    )
  elseif buffer_info.format == "jsonl" then
    -- Write JSONL to temp file
    temp_path, file_err = write_temp_file(buffer_info.content, ".jsonl")
    if not temp_path then
      return false, file_err
    end

    -- Track temp file for cleanup
    table.insert(connection.temp_files, temp_path)

    create_query = string.format(
      "CREATE TEMP TABLE %s AS SELECT * FROM read_json('%s', format='newline_delimited')",
      table_name,
      temp_path
    )
  else
    return false, string.format("Unsupported format: %s", buffer_info.format)
  end

  local _, err = M.execute_query(connection, create_query)
  if err then
    return false, string.format("Failed to load buffer data: %s", err)
  end

  return true
end

-- ============================================================================
-- Buffer Query Execution
-- ============================================================================

---Execute query on buffer(s)
---@param query string SQL query
---@param buffer_identifier string|number|nil Buffer identifier
---@return QueryResult? result
---@return string? error
function M.query_buffer(query, buffer_identifier)
  -- Create connection
  local conn, err = M.create_connection()
  if not conn then
    return nil, err
  end

  -- Ensure cleanup happens
  local success, result, error_msg = pcall(function()
    -- Extract buffer references from query
    local identifiers = buffer_module.extract_buffer_references(query)

    -- If no buffer references found, use provided identifier or current buffer
    if #identifiers == 0 then
      identifiers = { buffer_identifier or vim.api.nvim_get_current_buf() }
    end

    -- Load all referenced buffers
    local loaded_tables = {}
    for _, id in ipairs(identifiers) do
      local buffer_info, buf_err = buffer_module.get_buffer_info(id)
      if not buffer_info then
        error(buf_err)
      end

      -- Validate content
      local valid, val_err = buffer_module.validate_content(buffer_info.content, buffer_info.format)
      if not valid then
        error(string.format("Invalid %s content: %s", buffer_info.format, val_err))
      end

      -- Generate table name
      local table_name
      if type(id) == "string" then
        table_name = id:gsub("[^%w_]", "_")
      else
        local basename = vim.fn.fnamemodify(buffer_info.name, ":t:r")
        table_name = basename ~= "" and basename or "buffer"
      end

      -- Load buffer data
      local load_ok, load_err = M.load_buffer_data(conn, table_name, buffer_info)
      if not load_ok then
        error(load_err)
      end

      table.insert(loaded_tables, table_name)
    end

    -- Replace buffer() references with actual table names in query
    local processed_query = query
    for i, id in ipairs(identifiers) do
      local table_name = loaded_tables[i]
      -- Replace buffer('name') or buffer(num) with table name
      if type(id) == "string" then
        processed_query =
          processed_query:gsub("buffer%s*%(%s*['\"]" .. id:gsub("[%-%.]", "%%%1") .. "['\"]%s*%)", table_name)
      else
        processed_query = processed_query:gsub("buffer%s*%(%s*" .. id .. "%s*%)", table_name)
      end
    end

    -- Replace standalone 'buffer' references if we loaded a single table
    if #loaded_tables == 1 then
      processed_query = processed_query:gsub("%f[%w]buffer%f[%W]", loaded_tables[1])
    end

    -- Execute the query
    local query_result, query_err = M.execute_query(conn, processed_query)
    if not query_result then
      error(query_err)
    end

    return query_result
  end)

  M.close_connection(conn)

  if not success then
    return nil, tostring(result)
  end

  return result, error_msg
end

return M
