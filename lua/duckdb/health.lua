---@class DuckDBHealth
local M = {}

local health = vim.health or require('health')

---Check if LuaJIT is available
---@return boolean
local function check_luajit()
  if jit then
    health.ok(string.format('LuaJIT %s detected', jit.version))
    return true
  else
    health.error('LuaJIT not found', {
      'This plugin requires LuaJIT',
      'Make sure you are using Neovim (which includes LuaJIT by default)',
    })
    return false
  end
end

---Check if FFI is available
---@return boolean
local function check_ffi()
  local ok, ffi = pcall(require, 'ffi')
  if ok then
    health.ok('FFI module available')
    return true
  else
    health.error('FFI module not available', {
      'LuaJIT FFI is required for this plugin',
      'Error: ' .. tostring(ffi),
    })
    return false
  end
end

---Check DuckDB version meets minimum requirement
---@param conn any DuckDB connection
---@param query_module any Query module
---@return boolean meets_requirement
---@return string? version
local function check_duckdb_version(conn, query_module)
  local result = query_module.execute_query(conn, "SELECT version()")
  if result and result.rows[1] and result.rows[1][1] then
    local version_str = result.rows[1][1]
    local major, minor = version_str:match("v?(%d+)%.(%d+)")
    if major and minor then
      major, minor = tonumber(major), tonumber(minor)
      if major > 0 or (major == 0 and minor >= 9) then
        return true, version_str
      else
        return false, version_str
      end
    end
    return true, version_str
  end
  return true, nil
end

---Check if DuckDB library is available
---@return boolean
local function check_duckdb_lib()
  local ok, duckdb_ffi = pcall(require, 'duckdb.ffi')
  if not ok then
    health.error('Failed to load duckdb.ffi module', {
      'Error: ' .. tostring(duckdb_ffi),
    })
    return false
  end

  local available, err = duckdb_ffi.is_available()
  if available then
    health.ok('DuckDB library found and loaded successfully')
    return true
  else
    health.error('DuckDB library not found', {
      err or 'Unknown error',
      'Please install DuckDB 0.9.0 or later:',
      '  - Ubuntu/Debian: sudo apt install libduckdb-dev',
      '  - macOS: brew install duckdb',
      '  - Arch Linux: sudo pacman -S duckdb',
      '  - Or download from: https://duckdb.org/docs/installation/',
    })
    return false
  end
end

---Test basic DuckDB functionality
---@return boolean
local function test_basic_query()
  local ok, query_module = pcall(require, 'duckdb.query')
  if not ok then
    health.error('Failed to load duckdb.query module', {
      'Error: ' .. tostring(query_module),
    })
    return false
  end

  -- Try to create a connection
  local conn, err = query_module.create_connection()
  if not conn then
    health.error('Failed to create DuckDB connection', {
      err or 'Unknown error',
    })
    return false
  end

  -- Check DuckDB version
  local meets_version, version = check_duckdb_version(conn, query_module)
  if version then
    if meets_version then
      health.ok(string.format('DuckDB version: %s (meets minimum 0.9.0)', version))
    else
      health.error(string.format('DuckDB version %s is too old', version), {
        'This plugin requires DuckDB 0.9.0 or later',
        'Please upgrade DuckDB to use this plugin',
      })
      query_module.close_connection(conn)
      return false
    end
  end

  -- Try a simple query
  local result, query_err = query_module.execute_query(conn, 'SELECT 42 as answer')
  query_module.close_connection(conn)

  if not result then
    health.error('Failed to execute test query', {
      query_err or 'Unknown error',
    })
    return false
  end

  if result.rows[1] and result.rows[1][1] == 42 then
    health.ok('Basic query execution works correctly')
    return true
  else
    health.error('Test query returned unexpected result')
    return false
  end
end

---Check CSV parsing capability
---@return boolean
local function test_csv_parsing()
  local ok, query_module = pcall(require, 'duckdb.query')
  if not ok then
    return false
  end

  local conn = query_module.create_connection()
  if not conn then
    health.warn('Cannot test CSV parsing: connection failed')
    return false
  end

  local test_csv = "name,age,city\nAlice,30,NYC\nBob,25,LA"
  local csv_query = string.format(
    "SELECT * FROM read_csv_auto(%s)",
    vim.inspect(test_csv)
  )

  local result, query_err = query_module.execute_query(conn, csv_query)
  query_module.close_connection(conn)

  if result and result.row_count == 2 then
    health.ok('CSV parsing works correctly')
    return true
  else
    health.warn('CSV parsing test failed: ' .. (query_err or 'unexpected result'))
    return false
  end
end

---Check JSON parsing capability
---@return boolean
local function test_json_parsing()
  local ok, query_module = pcall(require, 'duckdb.query')
  if not ok then
    return false
  end

  local conn = query_module.create_connection()
  if not conn then
    health.warn('Cannot test JSON parsing: connection failed')
    return false
  end

  local test_json = '[{"name":"Alice","age":30},{"name":"Bob","age":25}]'
  local json_query = string.format(
    "SELECT * FROM read_json_auto(%s)",
    vim.inspect(test_json)
  )

  local result, query_err = query_module.execute_query(conn, json_query)
  query_module.close_connection(conn)

  if result and result.row_count == 2 then
    health.ok('JSON parsing works correctly')
    return true
  else
    health.warn('JSON parsing test failed: ' .. (query_err or 'unexpected result'))
    return false
  end
end

---Run all health checks
function M.check()
  health.start('DuckDB Plugin Health Check')

  -- Show version
  local duckdb_ok, duckdb_module = pcall(require, 'duckdb')
  if duckdb_ok and duckdb_module.VERSION then
    health.info(string.format('Plugin version: %s', duckdb_module.VERSION))
  end

  -- Check prerequisites
  health.start('Prerequisites')
  local has_luajit = check_luajit()
  local has_ffi = check_ffi()

  if not has_luajit or not has_ffi then
    health.start('Conclusion')
    health.error('Prerequisites not met - plugin will not work')
    return
  end

  -- Check DuckDB
  health.start('DuckDB Library')
  local has_duckdb = check_duckdb_lib()

  if not has_duckdb then
    health.start('Conclusion')
    health.error('DuckDB library not available - plugin will not work')
    return
  end

  -- Test functionality
  health.start('Functionality Tests')
  local basic_works = test_basic_query()

  if basic_works then
    test_csv_parsing()
    test_json_parsing()
  end

  -- Final status
  health.start('Conclusion')
  if has_luajit and has_ffi and has_duckdb and basic_works then
    health.ok('DuckDB plugin is fully functional')
  else
    health.warn('Some features may not work correctly')
  end
end

return M
