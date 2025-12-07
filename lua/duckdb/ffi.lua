---@class DuckDBFFI
---@field C table FFI C namespace
local M = {}

local ffi = require('ffi')

-- Guard against multiple ffi.cdef calls (LuaJIT doesn't allow redefining types)
-- This is necessary for test environments that reload modules
if not pcall(ffi.typeof, 'duckdb_database') then
  -- DuckDB C API declarations (Modern API - Nov 2025)
  -- Source: https://github.com/duckdb/duckdb/blob/main/src/include/duckdb.h
  ffi.cdef[[
    // Basic type definitions
    typedef uint64_t idx_t;

    // Type enumeration (complete as of Nov 2025)
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
      DUCKDB_TYPE_UHUGEINT = 32,
      DUCKDB_TYPE_ARRAY = 33,
      DUCKDB_TYPE_ANY = 34,
      DUCKDB_TYPE_VARINT = 35,
      DUCKDB_TYPE_SQLNULL = 36,
    } duckdb_type;

    typedef enum {
      DuckDBSuccess = 0,
      DuckDBError = 1
    } duckdb_state;

    // Opaque handle types (modern API uses void*)
    typedef void *duckdb_database;
    typedef void *duckdb_connection;
    typedef void *duckdb_data_chunk;
    typedef void *duckdb_vector;
    typedef void *duckdb_logical_type;
    typedef void *duckdb_prepared_statement;

    // String structure with inline optimization
    // Strings <= 12 bytes are stored inline, larger ones use pointer
    typedef struct {
      union {
        struct {
          uint32_t length;
          char prefix[4];
          char *ptr;
        } pointer;
        struct {
          uint32_t length;
          char inlined[12];
        } inlined;
      } value;
    } duckdb_string_t;

    // Date/time structures
    typedef struct { int32_t days; } duckdb_date;
    typedef struct { int64_t micros; } duckdb_time;
    typedef struct { int64_t micros; } duckdb_timestamp;
    typedef struct { int32_t months; int32_t days; int64_t micros; } duckdb_interval;
    typedef struct { uint64_t lower; int64_t upper; } duckdb_hugeint;
    typedef struct { uint64_t lower; uint64_t upper; } duckdb_uhugeint;

    // Result struct - use accessor functions, not direct field access
    typedef struct {
      idx_t deprecated_column_count;
      idx_t deprecated_row_count;
      idx_t deprecated_rows_changed;
      void *deprecated_columns;
      char *deprecated_error_message;
      void *internal_data;
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

    // Result error handling
    const char* duckdb_result_error(duckdb_result* result);

    // Result metadata accessors
    const char* duckdb_column_name(duckdb_result* result, idx_t col);
    duckdb_type duckdb_column_type(duckdb_result* result, idx_t col);
    duckdb_logical_type duckdb_column_logical_type(duckdb_result* result, idx_t col);
    idx_t duckdb_column_count(duckdb_result* result);
    idx_t duckdb_row_count(duckdb_result* result);
    idx_t duckdb_rows_changed(duckdb_result* result);

    // Data Chunk API (modern approach for reading results)
    duckdb_data_chunk duckdb_fetch_chunk(duckdb_result result);
    idx_t duckdb_data_chunk_get_size(duckdb_data_chunk chunk);
    idx_t duckdb_data_chunk_get_column_count(duckdb_data_chunk chunk);
    duckdb_vector duckdb_data_chunk_get_vector(duckdb_data_chunk chunk, idx_t col_idx);
    void duckdb_destroy_data_chunk(duckdb_data_chunk *chunk);

    // Vector API (column data access)
    void *duckdb_vector_get_data(duckdb_vector vector);
    uint64_t *duckdb_vector_get_validity(duckdb_vector vector);
    duckdb_logical_type duckdb_vector_get_column_type(duckdb_vector vector);

    // Validity mask operations
    bool duckdb_validity_row_is_valid(uint64_t *validity, idx_t row);

    // Logical type operations
    duckdb_type duckdb_get_type_id(duckdb_logical_type type);
    void duckdb_destroy_logical_type(duckdb_logical_type *type);

    // Prepared statements
    duckdb_state duckdb_prepare(duckdb_connection connection, const char* query, duckdb_prepared_statement* out_prepared_statement);
    void duckdb_destroy_prepare(duckdb_prepared_statement* prepared_statement);
    duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool val);
    duckdb_state duckdb_bind_int8(duckdb_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
    duckdb_state duckdb_bind_int16(duckdb_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
    duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t val);
    duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t val);
    duckdb_state duckdb_bind_uint8(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint8_t val);
    duckdb_state duckdb_bind_uint16(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint16_t val);
    duckdb_state duckdb_bind_uint32(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint32_t val);
    duckdb_state duckdb_bind_uint64(duckdb_prepared_statement prepared_statement, idx_t param_idx, uint64_t val);
    duckdb_state duckdb_bind_float(duckdb_prepared_statement prepared_statement, idx_t param_idx, float val);
    duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double val);
    duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char* val);
    duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);
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

---Type name mapping (complete as of Nov 2025)
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
  [28] = "UNION",
  [29] = "BIT",
  [30] = "TIME_TZ",
  [31] = "TIMESTAMP_TZ",
  [32] = "UHUGEINT",
  [33] = "ARRAY",
  [34] = "ANY",
  [35] = "VARINT",
  [36] = "SQLNULL",
}

---Type constants for cleaner code
M.types = {
  INVALID = 0,
  BOOLEAN = 1,
  TINYINT = 2,
  SMALLINT = 3,
  INTEGER = 4,
  BIGINT = 5,
  UTINYINT = 6,
  USMALLINT = 7,
  UINTEGER = 8,
  UBIGINT = 9,
  FLOAT = 10,
  DOUBLE = 11,
  TIMESTAMP = 12,
  DATE = 13,
  TIME = 14,
  INTERVAL = 15,
  HUGEINT = 16,
  VARCHAR = 17,
  BLOB = 18,
  DECIMAL = 19,
  TIMESTAMP_S = 20,
  TIMESTAMP_MS = 21,
  TIMESTAMP_NS = 22,
  ENUM = 23,
  LIST = 24,
  STRUCT = 25,
  MAP = 26,
  UUID = 27,
  UNION = 28,
  BIT = 29,
  TIME_TZ = 30,
  TIMESTAMP_TZ = 31,
  UHUGEINT = 32,
  ARRAY = 33,
  ANY = 34,
  VARINT = 35,
  SQLNULL = 36,
}

---Extract string from duckdb_string_t structure
---Handles both inline (<=12 bytes) and pointer-based storage
---@param str_ptr ffi.cdata* Pointer to duckdb_string_t
---@return string
function M.extract_string(str_ptr)
  local len = str_ptr.value.inlined.length
  if len <= 12 then
    -- Inline string (not null-terminated, use explicit length)
    return ffi.string(str_ptr.value.inlined.inlined, len)
  else
    -- Pointer-based string
    return ffi.string(str_ptr.value.pointer.ptr, str_ptr.value.pointer.length)
  end
end

return M
