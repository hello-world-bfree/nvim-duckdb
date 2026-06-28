---@class DuckDBFFI
---@field C table FFI C namespace
local M = {}

local ffi = require('ffi')

-- Guard against multiple ffi.cdef calls (LuaJIT doesn't allow redefining types)
-- This is necessary for test environments that reload modules
if not pcall(ffi.typeof, 'duckdb_database') then
  -- DuckDB C API declarations (Modern API - v1.5.4)
  -- Source: https://github.com/duckdb/duckdb/blob/main/src/include/duckdb.h
  ffi.cdef[[
    // Basic type definitions
    typedef uint64_t idx_t;

    // Type enumeration (complete as of v1.5.4)
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
      DUCKDB_TYPE_BIGNUM = 35,
      DUCKDB_TYPE_SQLNULL = 36,
      DUCKDB_TYPE_STRING_LITERAL = 37,
      DUCKDB_TYPE_INTEGER_LITERAL = 38,
      DUCKDB_TYPE_TIME_NS = 39,
      DUCKDB_TYPE_GEOMETRY = 40,
      DUCKDB_TYPE_VARIANT = 41,
    } duckdb_type;

    typedef enum {
      DuckDBSuccess = 0,
      DuckDBError = 1
    } duckdb_state;

    // Error category enumeration (as of v1.5.4)
    typedef enum duckdb_error_type {
      DUCKDB_ERROR_INVALID = 0,
      DUCKDB_ERROR_OUT_OF_RANGE = 1,
      DUCKDB_ERROR_CONVERSION = 2,
      DUCKDB_ERROR_UNKNOWN_TYPE = 3,
      DUCKDB_ERROR_DECIMAL = 4,
      DUCKDB_ERROR_MISMATCH_TYPE = 5,
      DUCKDB_ERROR_DIVIDE_BY_ZERO = 6,
      DUCKDB_ERROR_OBJECT_SIZE = 7,
      DUCKDB_ERROR_INVALID_TYPE = 8,
      DUCKDB_ERROR_SERIALIZATION = 9,
      DUCKDB_ERROR_TRANSACTION = 10,
      DUCKDB_ERROR_NOT_IMPLEMENTED = 11,
      DUCKDB_ERROR_EXPRESSION = 12,
      DUCKDB_ERROR_CATALOG = 13,
      DUCKDB_ERROR_PARSER = 14,
      DUCKDB_ERROR_PLANNER = 15,
      DUCKDB_ERROR_SCHEDULER = 16,
      DUCKDB_ERROR_EXECUTOR = 17,
      DUCKDB_ERROR_CONSTRAINT = 18,
      DUCKDB_ERROR_INDEX = 19,
      DUCKDB_ERROR_STAT = 20,
      DUCKDB_ERROR_CONNECTION = 21,
      DUCKDB_ERROR_SYNTAX = 22,
      DUCKDB_ERROR_SETTINGS = 23,
      DUCKDB_ERROR_BINDER = 24,
      DUCKDB_ERROR_NETWORK = 25,
      DUCKDB_ERROR_OPTIMIZER = 26,
      DUCKDB_ERROR_NULL_POINTER = 27,
      DUCKDB_ERROR_IO = 28,
      DUCKDB_ERROR_INTERRUPT = 29,
      DUCKDB_ERROR_FATAL = 30,
      DUCKDB_ERROR_INTERNAL = 31,
      DUCKDB_ERROR_INVALID_INPUT = 32,
      DUCKDB_ERROR_OUT_OF_MEMORY = 33,
      DUCKDB_ERROR_PERMISSION = 34,
      DUCKDB_ERROR_PARAMETER_NOT_RESOLVED = 35,
      DUCKDB_ERROR_PARAMETER_NOT_ALLOWED = 36,
      DUCKDB_ERROR_DEPENDENCY = 37,
      DUCKDB_ERROR_HTTP = 38,
      DUCKDB_ERROR_MISSING_EXTENSION = 39,
      DUCKDB_ERROR_AUTOLOAD = 40,
      DUCKDB_ERROR_SEQUENCE = 41,
      DUCKDB_INVALID_CONFIGURATION = 42
    } duckdb_error_type;

    // Opaque handle types (modern API uses void*)
    typedef void *duckdb_database;
    typedef void *duckdb_connection;
    typedef void *duckdb_data_chunk;
    typedef void *duckdb_vector;
    typedef void *duckdb_logical_type;
    typedef void *duckdb_prepared_statement;

    // LIST metadata entry (offset + length into the child vector)
    typedef struct { uint64_t offset; uint64_t length; } duckdb_list_entry;

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
    duckdb_error_type duckdb_result_error_type(duckdb_result* result);

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

    // Nested logical type accessors (LIST / ARRAY / STRUCT / MAP / ENUM)
    duckdb_logical_type duckdb_list_type_child_type(duckdb_logical_type type);
    duckdb_logical_type duckdb_array_type_child_type(duckdb_logical_type type);
    idx_t duckdb_array_type_array_size(duckdb_logical_type type);
    idx_t duckdb_struct_type_child_count(duckdb_logical_type type);
    char *duckdb_struct_type_child_name(duckdb_logical_type type, idx_t index);
    duckdb_logical_type duckdb_struct_type_child_type(duckdb_logical_type type, idx_t index);
    duckdb_logical_type duckdb_map_type_key_type(duckdb_logical_type type);
    duckdb_logical_type duckdb_map_type_value_type(duckdb_logical_type type);
    duckdb_type duckdb_enum_internal_type(duckdb_logical_type type);
    char *duckdb_enum_dictionary_value(duckdb_logical_type type, idx_t index);

    // Nested vector accessors (descend into child vectors)
    duckdb_vector duckdb_list_vector_get_child(duckdb_vector vector);
    duckdb_vector duckdb_array_vector_get_child(duckdb_vector vector);
    duckdb_vector duckdb_struct_vector_get_child(duckdb_vector vector, idx_t index);

    // Decimal logical type accessors (width/scale/internal storage type)
    uint8_t duckdb_decimal_width(duckdb_logical_type type);
    uint8_t duckdb_decimal_scale(duckdb_logical_type type);
    duckdb_type duckdb_decimal_internal_type(duckdb_logical_type type);

    // Library version
    const char *duckdb_library_version();

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

---Type name mapping (complete as of v1.5.4)
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
  [35] = "BIGNUM",
  [36] = "SQLNULL",
  [37] = "STRING_LITERAL",
  [38] = "INTEGER_LITERAL",
  [39] = "TIME_NS",
  [40] = "GEOMETRY",
  [41] = "VARIANT",
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
  BIGNUM = 35,
  SQLNULL = 36,
  STRING_LITERAL = 37,
  INTEGER_LITERAL = 38,
  TIME_NS = 39,
  GEOMETRY = 40,
  VARIANT = 41,
}

---Error category names indexed by duckdb_error_type enum value (as of v1.5.4)
M.error_type_names = {
  [0] = "INVALID",
  [1] = "OUT_OF_RANGE",
  [2] = "CONVERSION",
  [3] = "UNKNOWN_TYPE",
  [4] = "DECIMAL",
  [5] = "MISMATCH_TYPE",
  [6] = "DIVIDE_BY_ZERO",
  [7] = "OBJECT_SIZE",
  [8] = "INVALID_TYPE",
  [9] = "SERIALIZATION",
  [10] = "TRANSACTION",
  [11] = "NOT_IMPLEMENTED",
  [12] = "EXPRESSION",
  [13] = "CATALOG",
  [14] = "PARSER",
  [15] = "PLANNER",
  [16] = "SCHEDULER",
  [17] = "EXECUTOR",
  [18] = "CONSTRAINT",
  [19] = "INDEX",
  [20] = "STAT",
  [21] = "CONNECTION",
  [22] = "SYNTAX",
  [23] = "SETTINGS",
  [24] = "BINDER",
  [25] = "NETWORK",
  [26] = "OPTIMIZER",
  [27] = "NULL_POINTER",
  [28] = "IO",
  [29] = "INTERRUPT",
  [30] = "FATAL",
  [31] = "INTERNAL",
  [32] = "INVALID_INPUT",
  [33] = "OUT_OF_MEMORY",
  [34] = "PERMISSION",
  [35] = "PARAMETER_NOT_RESOLVED",
  [36] = "PARAMETER_NOT_ALLOWED",
  [37] = "DEPENDENCY",
  [38] = "HTTP",
  [39] = "MISSING_EXTENSION",
  [40] = "AUTOLOAD",
  [41] = "SEQUENCE",
  [42] = "INVALID_CONFIGURATION",
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
