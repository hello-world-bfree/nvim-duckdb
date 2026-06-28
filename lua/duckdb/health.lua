---@class DuckDBHealth
local M = {}

local health = vim.health or require('health')

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
    local ver_ok, version = pcall(function()
      local ffi = require('ffi')
      return ffi.string(duckdb_ffi.C.duckdb_library_version())
    end)
    if ver_ok and version then
      health.info(string.format('libduckdb C API version: %s', version))
    end
    return true
  else
    health.error('DuckDB library not found', {
      err or 'Unknown error',
      'Please install DuckDB 0.10.0 or later:',
      '  - Ubuntu/Debian: sudo apt install libduckdb-dev',
      '  - macOS: brew install duckdb',
      '  - Arch Linux: sudo pacman -S duckdb',
      '  - Or download from: https://duckdb.org/docs/installation/',
    })
    return false
  end
end

local function check_duckdb_connection()
  local ok, query_module = pcall(require, 'duckdb.query')
  if not ok then
    health.error('Failed to load duckdb.query module', {
      'Error: ' .. tostring(query_module),
    })
    return false
  end

  local conn, err = query_module.create_connection()
  if not conn then
    health.error('Failed to create DuckDB connection', {
      err or 'Unknown error',
    })
    return false
  end

  local result = query_module.execute_query(conn, "SELECT version()")
  query_module.close_connection(conn)

  if result and result.rows[1] and result.rows[1][1] then
    local version_str = result.rows[1][1]
    local major, minor = version_str:match("v?(%d+)%.(%d+)")
    if major and minor then
      major, minor = tonumber(major), tonumber(minor)
      if major > 0 or (major == 0 and minor >= 10) then
        health.ok(string.format('DuckDB version: %s (meets minimum 0.10.0)', version_str))
        return true
      else
        health.error(string.format('DuckDB version %s is too old', version_str), {
          'This plugin requires DuckDB 0.10.0 or later',
          'Please upgrade DuckDB to use this plugin',
        })
        return false
      end
    end
    health.ok(string.format('DuckDB version: %s', version_str))
    return true
  end

  health.warn('Could not determine DuckDB version')
  return true
end

function M.check()
  health.start('DuckDB Plugin Health Check')

  local duckdb_ok, duckdb_module = pcall(require, 'duckdb')
  if duckdb_ok and duckdb_module.VERSION then
    health.info(string.format('Plugin version: %s', duckdb_module.VERSION))
  end

  health.start('Prerequisites')
  local has_luajit = check_luajit()
  local has_ffi = check_ffi()

  if not has_luajit or not has_ffi then
    return
  end

  health.start('DuckDB Library')
  local has_duckdb = check_duckdb_lib()

  if not has_duckdb then
    return
  end

  health.start('DuckDB Connection')
  local conn_ok = check_duckdb_connection()

  if has_luajit and has_ffi and has_duckdb and conn_ok then
    health.ok('DuckDB plugin is ready')
  end
end

return M
