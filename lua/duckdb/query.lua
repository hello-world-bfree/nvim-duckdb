---@class DuckDBQuery
local M = {}

local ffi = require('ffi')
local duckdb_ffi = require('duckdb.ffi')
local buffer_module = require('duckdb.buffer')

---@class DuckDBConnection
---@field db ffi.cdata* Database handle
---@field conn ffi.cdata* Connection handle
---@field temp_dir string? Temporary directory for data files
---@field _closed boolean Whether the connection has been closed

---@class QueryResult
---@field columns table<string> Column names
---@field rows table<table> Row data
---@field row_count number Number of rows
---@field column_count number Number of columns
---@field rows_changed number Number of rows affected (for DML)

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
    _closed = false,
  }

  return connection
end

---Close DuckDB connection
---@param connection DuckDBConnection
function M.close_connection(connection)
  -- Prevent double-close which can cause crashes
  if connection._closed then
    return
  end
  connection._closed = true

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

---Execute a query and return results
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
    if result[0].error_message ~= nil then
      error_msg = ffi.string(result[0].error_message)
    end
    duckdb_ffi.C.duckdb_destroy_result(result)
    return nil, error_msg
  end

  -- Extract column information
  local column_count = tonumber(duckdb_ffi.C.duckdb_column_count(result))
  local row_count = tonumber(duckdb_ffi.C.duckdb_row_count(result))
  local rows_changed = tonumber(duckdb_ffi.C.duckdb_rows_changed(result))

  local columns = {}
  for col = 0, column_count - 1 do
    local col_name = duckdb_ffi.C.duckdb_column_name(result, col)
    if col_name ~= nil then
      table.insert(columns, ffi.string(col_name))
    else
      table.insert(columns, string.format("column_%d", col))
    end
  end

  -- Extract row data
  local rows = {}
  for row = 0, row_count - 1 do
    local row_data = {}
    for col = 0, column_count - 1 do
      local value = M.get_value(result, col, row)
      table.insert(row_data, value)
    end
    table.insert(rows, row_data)
  end

  duckdb_ffi.C.duckdb_destroy_result(result)

  return {
    columns = columns,
    rows = rows,
    row_count = row_count,
    column_count = column_count,
    rows_changed = rows_changed,
  }
end

---Get value from result set
---@param result ffi.cdata*
---@param col number Column index
---@param row number Row index
---@return any value
function M.get_value(result, col, row)
  -- Check if value is null
  if duckdb_ffi.C.duckdb_value_is_null(result, col, row) then
    return nil
  end

  local col_type = duckdb_ffi.C.duckdb_column_type(result, col)

  -- Handle different types
  if col_type == 1 then -- BOOLEAN
    return duckdb_ffi.C.duckdb_value_boolean(result, col, row)
  elseif col_type == 2 then -- TINYINT
    return tonumber(duckdb_ffi.C.duckdb_value_int8(result, col, row))
  elseif col_type == 3 then -- SMALLINT
    return tonumber(duckdb_ffi.C.duckdb_value_int16(result, col, row))
  elseif col_type == 4 then -- INTEGER
    return tonumber(duckdb_ffi.C.duckdb_value_int32(result, col, row))
  elseif col_type == 5 then -- BIGINT
    return tonumber(duckdb_ffi.C.duckdb_value_int64(result, col, row))
  elseif col_type == 6 then -- UTINYINT
    return tonumber(duckdb_ffi.C.duckdb_value_uint8(result, col, row))
  elseif col_type == 7 then -- USMALLINT
    return tonumber(duckdb_ffi.C.duckdb_value_uint16(result, col, row))
  elseif col_type == 8 then -- UINTEGER
    return tonumber(duckdb_ffi.C.duckdb_value_uint32(result, col, row))
  elseif col_type == 9 then -- UBIGINT
    return tonumber(duckdb_ffi.C.duckdb_value_uint64(result, col, row))
  elseif col_type == 10 then -- FLOAT
    return tonumber(duckdb_ffi.C.duckdb_value_float(result, col, row))
  elseif col_type == 11 then -- DOUBLE
    return tonumber(duckdb_ffi.C.duckdb_value_double(result, col, row))
  elseif col_type == 17 or col_type == 28 then -- VARCHAR or JSON
    local str_ptr = duckdb_ffi.C.duckdb_value_varchar(result, col, row)
    if str_ptr ~= nil then
      local str = ffi.string(str_ptr)
      duckdb_ffi.C.duckdb_free(str_ptr)
      return str
    end
    return ""
  else
    -- For other types, try to get as varchar
    local str_ptr = duckdb_ffi.C.duckdb_value_varchar(result, col, row)
    if str_ptr ~= nil then
      local str = ffi.string(str_ptr)
      duckdb_ffi.C.duckdb_free(str_ptr)
      return str
    end
    return nil
  end
end

---Load buffer data into a table
---@param connection DuckDBConnection
---@param table_name string Table name
---@param buffer_info BufferInfo Buffer information
---@return boolean success
---@return string? error
function M.load_buffer_data(connection, table_name, buffer_info)
  local create_query

  if buffer_info.format == 'csv' then
    -- Escape single quotes in content
    local escaped_content = buffer_info.content:gsub("'", "''")

    -- Use DuckDB's read_csv_auto to auto-detect schema
    create_query = string.format(
      "CREATE TEMPORARY TABLE %s AS SELECT * FROM read_csv_auto('%s')",
      table_name,
      '/dev/stdin'
    )

    -- Alternative: use inline CSV data
    -- DuckDB supports reading from string values
    create_query = string.format(
      "CREATE TEMPORARY TABLE %s AS SELECT * FROM read_csv_auto(%s, sample_size=-1)",
      table_name,
      vim.inspect(buffer_info.content)
    )

  elseif buffer_info.format == 'json' then
    -- Use DuckDB's read_json_auto
    create_query = string.format(
      "CREATE TEMPORARY TABLE %s AS SELECT * FROM read_json_auto(%s)",
      table_name,
      vim.inspect(buffer_info.content)
    )

  elseif buffer_info.format == 'jsonl' then
    -- Use DuckDB's read_json_auto with format='newline_delimited'
    create_query = string.format(
      "CREATE TEMPORARY TABLE %s AS SELECT * FROM read_json_auto(%s, format='newline_delimited')",
      table_name,
      vim.inspect(buffer_info.content)
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
      if type(id) == 'string' then
        table_name = id:gsub('[^%w_]', '_')
      else
        local basename = vim.fn.fnamemodify(buffer_info.name, ':t:r')
        table_name = basename ~= '' and basename or 'buffer'
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
      if type(id) == 'string' then
        processed_query = processed_query:gsub(
          "buffer%s*%(%s*['\"]" .. id:gsub("[%-%.]", "%%%1") .. "['\"]%s*%)",
          table_name
        )
      else
        processed_query = processed_query:gsub(
          "buffer%s*%(%s*" .. id .. "%s*%)",
          table_name
        )
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
