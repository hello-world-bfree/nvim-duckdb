---@class DuckDBFFI
---@field C table FFI C namespace
local M = {}

local ffi = require('ffi')

-- Guard against multiple ffi.cdef calls (LuaJIT doesn't allow redefining types)
-- This is necessary for test environments that reload modules
if not pcall(ffi.typeof, 'duckdb_database') then
  -- DuckDB C API declarations (modern opaque API)
  -- Note: duckdb_result is opaque - use accessor functions instead of direct struct access
  ffi.cdef[[
    // Type definitions
    typedef enum {
      DUCKDB_TYPE_INVALID = 0,
      DUCKDB_TYPE_BOOLEAN = 1,
      DUCKDB_TYPE_TINYINT = 2,
      DUCKDB_TYPE_SMALLINT = 3,
      DUCKDB_TYPE_INTEGER = 4,
      DUCKDB_TYPE_BIGINT = 5,
      DUCKDB_TYPE_UTINYINT = 6,
      DUCKDB_TYPE_USMALLINT = 7,
      DUCKDB_TYPE_UINTEGER = 8,
      DUCKDB_TYPE_UBIGINT = 9,
      DUCKDB_TYPE_FLOAT = 10,
      DUCKDB_TYPE_DOUBLE = 11,
      DUCKDB_TYPE_TIMESTAMP = 12,
      DUCKDB_TYPE_DATE = 13,
      DUCKDB_TYPE_TIME = 14,
      DUCKDB_TYPE_INTERVAL = 15,
      DUCKDB_TYPE_HUGEINT = 16,
      DUCKDB_TYPE_VARCHAR = 17,
      DUCKDB_TYPE_BLOB = 18,
      DUCKDB_TYPE_DECIMAL = 19,
      DUCKDB_TYPE_TIMESTAMP_S = 20,
      DUCKDB_TYPE_TIMESTAMP_MS = 21,
      DUCKDB_TYPE_TIMESTAMP_NS = 22,
      DUCKDB_TYPE_ENUM = 23,
      DUCKDB_TYPE_LIST = 24,
      DUCKDB_TYPE_STRUCT = 25,
      DUCKDB_TYPE_MAP = 26,
      DUCKDB_TYPE_UUID = 27,
      DUCKDB_TYPE_UNION = 28,
      DUCKDB_TYPE_BIT = 29,
      DUCKDB_TYPE_TIME_TZ = 30,
      DUCKDB_TYPE_TIMESTAMP_TZ = 31,
    } duckdb_type;

    typedef enum {
      DuckDBSuccess = 0,
      DuckDBError = 1
    } duckdb_state;

    // Opaque handle types
    typedef void* duckdb_database;
    typedef void* duckdb_connection;

    // Result is an opaque struct - internal layout is not exposed
    // Must use accessor functions to interact with it
    typedef struct {
      void* __deprecated_data;  // Internal use only
      void* __deprecated_nulls; // Internal use only
      void* __deprecated_types; // Internal use only
      void* __deprecated_names; // Internal use only
      void* internal_data;      // Opaque internal pointer
    } duckdb_result;

    // Database operations
    duckdb_state duckdb_open(const char* path, duckdb_database* out_database);
    duckdb_state duckdb_open_ext(const char* path, duckdb_database* out_database, void* config, char** out_error);
    void duckdb_close(duckdb_database* database);

    // Connection operations
    duckdb_state duckdb_connect(duckdb_database database, duckdb_connection* out_connection);
    void duckdb_disconnect(duckdb_connection* connection);

    // Query operations
    duckdb_state duckdb_query(duckdb_connection connection, const char* query, duckdb_result* out_result);
    void duckdb_destroy_result(duckdb_result* result);

    // Result error handling (modern API - use these instead of direct struct access)
    const char* duckdb_result_error(duckdb_result* result);

    // Result accessors (these work with the opaque struct)
    const char* duckdb_column_name(duckdb_result* result, uint64_t col);
    duckdb_type duckdb_column_type(duckdb_result* result, uint64_t col);
    uint64_t duckdb_column_count(duckdb_result* result);
    uint64_t duckdb_row_count(duckdb_result* result);
    uint64_t duckdb_rows_changed(duckdb_result* result);

    // Value accessors
    bool duckdb_value_boolean(duckdb_result* result, uint64_t col, uint64_t row);
    int8_t duckdb_value_int8(duckdb_result* result, uint64_t col, uint64_t row);
    int16_t duckdb_value_int16(duckdb_result* result, uint64_t col, uint64_t row);
    int32_t duckdb_value_int32(duckdb_result* result, uint64_t col, uint64_t row);
    int64_t duckdb_value_int64(duckdb_result* result, uint64_t col, uint64_t row);
    uint8_t duckdb_value_uint8(duckdb_result* result, uint64_t col, uint64_t row);
    uint16_t duckdb_value_uint16(duckdb_result* result, uint64_t col, uint64_t row);
    uint32_t duckdb_value_uint32(duckdb_result* result, uint64_t col, uint64_t row);
    uint64_t duckdb_value_uint64(duckdb_result* result, uint64_t col, uint64_t row);
    float duckdb_value_float(duckdb_result* result, uint64_t col, uint64_t row);
    double duckdb_value_double(duckdb_result* result, uint64_t col, uint64_t row);
    char* duckdb_value_varchar(duckdb_result* result, uint64_t col, uint64_t row);
    bool duckdb_value_is_null(duckdb_result* result, uint64_t col, uint64_t row);

    // Prepared statements
    typedef void* duckdb_prepared_statement;
    duckdb_state duckdb_prepare(duckdb_connection connection, const char* query, duckdb_prepared_statement* out_prepared_statement);
    void duckdb_destroy_prepare(duckdb_prepared_statement* prepared_statement);
    duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, uint64_t param_idx, bool val);
    duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, uint64_t param_idx, int8_t val);
    duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, uint64_t param_idx, int16_t val);
    duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, uint64_t param_idx, int32_t val);
    duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, uint64_t param_idx, int64_t val);
    duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, uint64_t param_idx, uint8_t val);
    duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, uint64_t param_idx, uint16_t val);
    duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, uint64_t param_idx, uint32_t val);
    duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, uint64_t param_idx, uint64_t val);
    duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, uint64_t param_idx, float val);
    duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, uint64_t param_idx, double val);
    duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, uint64_t param_idx, const char* val);
    duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, uint64_t param_idx);
    duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result* out_result);

    // Memory management
    void duckdb_free(void* ptr);
    void* duckdb_malloc(size_t size);
  ]]
end

-- Try to load DuckDB library
local function try_load_library()
  local lib_names = {
    'libduckdb.so',      -- Linux
    'libduckdb.dylib',   -- macOS
    'duckdb.dll',        -- Windows
    'duckdb',            -- Generic
  }

  for _, name in ipairs(lib_names) do
    local ok, lib = pcall(ffi.load, name)
    if ok then
      return lib
    end
  end

  return nil
end

M.lib = try_load_library()
M.C = M.lib

---Check if DuckDB library is available
---@return boolean available
---@return string? error
function M.is_available()
  if M.lib then
    return true
  end
  return false, "DuckDB library not found. Please install libduckdb."
end

---Type name mapping
M.type_names = {
  [0] = "INVALID",
  [1] = "BOOLEAN",
  [2] = "TINYINT",
  [3] = "SMALLINT",
  [4] = "INTEGER",
  [5] = "BIGINT",
  [6] = "UTINYINT",
  [7] = "USMALLINT",
  [8] = "UINTEGER",
  [9] = "UBIGINT",
  [10] = "FLOAT",
  [11] = "DOUBLE",
  [12] = "TIMESTAMP",
  [13] = "DATE",
  [14] = "TIME",
  [15] = "INTERVAL",
  [16] = "HUGEINT",
  [17] = "VARCHAR",
  [18] = "BLOB",
  [19] = "DECIMAL",
  [20] = "TIMESTAMP_S",
  [21] = "TIMESTAMP_MS",
  [22] = "TIMESTAMP_NS",
  [23] = "ENUM",
  [24] = "LIST",
  [25] = "STRUCT",
  [26] = "MAP",
  [27] = "UUID",
  [28] = "JSON",
}

return M
