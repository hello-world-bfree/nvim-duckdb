---@class DuckDBQuery
local M = {}

local ffi = require("ffi")
local bit = require("bit")
local duckdb_ffi = require("duckdb.ffi")
local buffer_module = require("duckdb.buffer")

-- Type constants for cleaner code
local T = duckdb_ffi.types

-- Sentinel for SQL NULL in result rows. Using a distinct non-nil value (rather
-- than Lua nil) keeps rows hole-free so that `ipairs(row)` and `#row` stay
-- correct even when a column is NULL. Renders as an empty string; encode it as
-- JSON null via M.is_null() at export sites.
M.NULL = setmetatable({}, {
  __tostring = function()
    return ""
  end,
})

---Test whether a result-row value is SQL NULL.
---@param value any
---@return boolean
function M.is_null(value)
  return value == M.NULL
end

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
---@field statement_type number duckdb_statement_type enum (1=SELECT, 7=CREATE, ...)

-- ============================================================================
-- SQL Identifier Helpers
-- ============================================================================

local function quote_identifier(name)
  return '"' .. name:gsub('"', '""') .. '"'
end

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

---Convert an unsigned 128-bit magnitude (upper:uint64, lower:uint64) to a
---base-10 string via repeated long-division by 10. LuaJIT lacks native 128-bit
---arithmetic, so we divide the two 64-bit halves manually, carrying the
---remainder from the high half into the low half.
---@param upper ffi.cdata* uint64_t high 64 bits
---@param lower ffi.cdata* uint64_t low 64 bits
---@return string
local function u128_to_decimal(upper, lower)
  if upper == 0 and lower == 0 then
    return "0"
  end

  local digits = {}
  -- Loop until the full 128-bit value reaches zero.
  while upper ~= 0 or lower ~= 0 do
    -- Divide high half; its remainder becomes the top bits of the low half.
    local up_q = upper / 10ULL
    local up_r = upper % 10ULL
    -- low_dividend = up_r * 2^64 + lower, computed as two unsigned divisions
    -- to avoid overflow: split 2^64 into (2^64 / 10) and (2^64 % 10).
    local lo_q = lower / 10ULL
    local lo_r = lower % 10ULL
    -- Contribution of the carried remainder (up_r) across the 2^64 boundary.
    local carry_q = up_r * 1844674407370955161ULL -- floor(2^64 / 10)
    local carry_r = up_r * 6ULL -- 2^64 mod 10 == 6
    lo_q = lo_q + carry_q + (lo_r + carry_r) / 10ULL
    local rem = (lo_r + carry_r) % 10ULL

    table.insert(digits, 1, tostring(tonumber(rem)))
    upper = up_q
    lower = lo_q
  end

  return table.concat(digits)
end

---Format a signed hugeint (128-bit) to its exact base-10 string.
---@param hugeint ffi.cdata* duckdb_hugeint structure {uint64 lower; int64 upper}
---@return string
local function format_hugeint(hugeint)
  local upper = hugeint.upper
  local lower = hugeint.lower

  if upper >= 0 then
    return u128_to_decimal(ffi.cast("uint64_t", upper), lower)
  end

  -- Negative: take the two's-complement magnitude of the full 128-bit value
  -- (~value + 1 across both 64-bit words) then prefix '-'. The carry from the
  -- low word propagates into the high word iff the negated low word wraps to 0.
  local mag_lower = bit.bnot(ffi.cast("uint64_t", lower)) + 1ULL
  local carry = (mag_lower == 0ULL) and 1ULL or 0ULL
  local mag_upper = bit.bnot(ffi.cast("uint64_t", upper)) + carry
  return "-" .. u128_to_decimal(mag_upper, mag_lower)
end

---Insert a decimal point `scale` digits from the right of an integer string.
---@param int_str string Unscaled integer (may start with '-')
---@param scale number Number of fractional digits
---@return string
local function apply_decimal_scale(int_str, scale)
  if scale == 0 then
    return int_str
  end

  local sign = ""
  if int_str:sub(1, 1) == "-" then
    sign = "-"
    int_str = int_str:sub(2)
  end

  if #int_str <= scale then
    int_str = string.rep("0", scale - #int_str + 1) .. int_str
  end

  local split = #int_str - scale
  return sign .. int_str:sub(1, split) .. "." .. int_str:sub(split + 1)
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
---@param decimal_info table? {internal_type, scale} for DECIMAL columns
---@return any value
local function extract_vector_value(vector, col_type, row, validity, decimal_info)
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
  elseif col_type == T.DECIMAL then
    -- DECIMAL is stored as a scaled integer; the storage width depends on
    -- precision (int16/int32/int64/hugeint). Read the raw integer per its
    -- internal type, then place the decimal point `scale` digits from the right.
    local internal = decimal_info and decimal_info.internal_type
    local scale = decimal_info and decimal_info.scale or 0
    local int_str
    if internal == T.SMALLINT then
      int_str = tostring(ffi.cast("int16_t*", data)[row])
    elseif internal == T.INTEGER then
      int_str = tostring(ffi.cast("int32_t*", data)[row])
    elseif internal == T.BIGINT then
      int_str = string.format("%d", ffi.cast("int64_t*", data)[row])
    elseif internal == T.HUGEINT then
      int_str = format_hugeint(ffi.cast("duckdb_hugeint*", data) + row)
    else
      return string.format("[%s]", duckdb_ffi.type_names[col_type] or "DECIMAL")
    end
    return apply_decimal_scale(int_str, scale)
  elseif col_type == T.UHUGEINT then
    local uhugeint_data = ffi.cast("duckdb_uhugeint*", data)
    return u128_to_decimal(uhugeint_data[row].upper, uhugeint_data[row].lower)
  elseif col_type == T.UUID then
    -- DuckDB stores UUID as a hugeint with the high bit flipped (so unsigned
    -- ordering matches). Flip it back to recover the true 128-bit value, then
    -- render as canonical RFC 4122 dashed hex (8-4-4-4-12).
    local uuid_data = ffi.cast("duckdb_hugeint*", data)
    local hi = bit.bxor(ffi.cast("uint64_t", uuid_data[row].upper), 0x8000000000000000ULL)
    local lo = ffi.cast("uint64_t", uuid_data[row].lower)
    local hex = string.format("%016x%016x", hi, lo)
    return string.format(
      "%s-%s-%s-%s-%s",
      hex:sub(1, 8),
      hex:sub(9, 12),
      hex:sub(13, 16),
      hex:sub(17, 20),
      hex:sub(21, 32)
    )
  else
    -- Truly unsupported leaf types fall back to a type-name placeholder.
    return string.format("[%s]", duckdb_ffi.type_names[col_type] or "UNKNOWN")
  end
end

local C = duckdb_ffi.C

---Destroy a logical type handle obtained from an accessor.
---@param logical ffi.cdata*
local function destroy_logical(logical)
  if logical ~= nil then
    local ptr = ffi.new("duckdb_logical_type[1]", logical)
    C.duckdb_destroy_logical_type(ptr)
  end
end

---Read a duckdb_free-owned C string and free it.
---@param c_str ffi.cdata* char* that must be freed with duckdb_free
---@return string
local function take_owned_string(c_str)
  if c_str == nil then
    return ""
  end
  local s = ffi.string(c_str)
  C.duckdb_free(c_str)
  return s
end

---Read an ENUM value: look up the dictionary string by the stored index.
---@param vector ffi.cdata*
---@param logical ffi.cdata* ENUM logical type
---@param row number
---@return string
local function extract_enum_value(vector, logical, row)
  local internal = tonumber(C.duckdb_enum_internal_type(logical))
  local data = C.duckdb_vector_get_data(vector)
  if data == nil then
    return ""
  end

  local idx
  if internal == T.UTINYINT then
    idx = tonumber(ffi.cast("uint8_t*", data)[row])
  elseif internal == T.USMALLINT then
    idx = tonumber(ffi.cast("uint16_t*", data)[row])
  else
    idx = tonumber(ffi.cast("uint32_t*", data)[row])
  end

  return take_owned_string(C.duckdb_enum_dictionary_value(logical, idx))
end

-- Forward declaration for mutual recursion.
local extract_value

---Extract one element from a child vector at a flat index, given the child's
---logical type. Handles NULL via the child vector's own validity mask.
---@param child_vector ffi.cdata*
---@param child_logical ffi.cdata* (NOT destroyed here; owned by caller)
---@param idx number Flat index into the child vector
---@return any
local function extract_child(child_vector, child_logical, idx)
  local validity = C.duckdb_vector_get_validity(child_vector)
  return extract_value(child_vector, child_logical, idx, validity)
end

---Recursively extract a value, descending into nested types.
---@param vector ffi.cdata*
---@param logical ffi.cdata* Logical type for this vector (owned by caller)
---@param row number Row index within this vector
---@param validity ffi.cdata*? Validity mask for this vector
---@return any
extract_value = function(vector, logical, row, validity)
  if validity ~= nil and not C.duckdb_validity_row_is_valid(validity, row) then
    return nil
  end

  local type_id = tonumber(C.duckdb_get_type_id(logical))

  if type_id == T.LIST then
    -- Row holds a {offset, length} entry into the flat child vector.
    local data = C.duckdb_vector_get_data(vector)
    local entry = ffi.cast("duckdb_list_entry*", data)[row]
    local child_vector = C.duckdb_list_vector_get_child(vector)
    local child_logical = C.duckdb_list_type_child_type(logical)
    local offset = tonumber(entry.offset)
    local length = tonumber(entry.length)
    -- Tag kind + length explicitly: list elements may be NULL (nil), which
    -- would create holes that ipairs/# cannot traverse reliably.
    local out = { __kind = "list", n = length }
    for i = 0, length - 1 do
      out[i + 1] = extract_child(child_vector, child_logical, offset + i)
    end
    destroy_logical(child_logical)
    return out
  elseif type_id == T.ARRAY then
    -- Fixed-size array: child elements are contiguous at row * size.
    local size = tonumber(C.duckdb_array_type_array_size(logical))
    local child_vector = C.duckdb_array_vector_get_child(vector)
    local child_logical = C.duckdb_array_type_child_type(logical)
    local base = row * size
    local out = { __kind = "list", n = size }
    for i = 0, size - 1 do
      out[i + 1] = extract_child(child_vector, child_logical, base + i)
    end
    destroy_logical(child_logical)
    return out
  elseif type_id == T.STRUCT then
    -- Each child is a parallel vector indexed by the same row.
    local count = tonumber(C.duckdb_struct_type_child_count(logical))
    local out = { __kind = "struct", fields = {} }
    for c = 0, count - 1 do
      local name = take_owned_string(C.duckdb_struct_type_child_name(logical, c))
      local child_vector = C.duckdb_struct_vector_get_child(vector, c)
      local child_logical = C.duckdb_struct_type_child_type(logical, c)
      out.fields[c + 1] = { name = name, value = extract_child(child_vector, child_logical, row) }
      destroy_logical(child_logical)
    end
    return out
  elseif type_id == T.MAP then
    -- A MAP is stored as a LIST of STRUCT(key, value). Descend the list entry,
    -- then read the two struct children for each element.
    local data = C.duckdb_vector_get_data(vector)
    local entry = ffi.cast("duckdb_list_entry*", data)[row]
    local list_child = C.duckdb_list_vector_get_child(vector) -- STRUCT vector
    local key_vector = C.duckdb_struct_vector_get_child(list_child, 0)
    local value_vector = C.duckdb_struct_vector_get_child(list_child, 1)
    local key_logical = C.duckdb_map_type_key_type(logical)
    local value_logical = C.duckdb_map_type_value_type(logical)
    local offset = tonumber(entry.offset)
    local length = tonumber(entry.length)
    local out = { __kind = "map", n = length }
    for i = 0, length - 1 do
      out[i + 1] = {
        key = extract_child(key_vector, key_logical, offset + i),
        value = extract_child(value_vector, value_logical, offset + i),
      }
    end
    destroy_logical(key_logical)
    destroy_logical(value_logical)
    return out
  elseif type_id == T.ENUM then
    return extract_enum_value(vector, logical, row)
  elseif type_id == T.DECIMAL then
    return extract_vector_value(vector, type_id, row, nil, {
      scale = tonumber(C.duckdb_decimal_scale(logical)),
      internal_type = tonumber(C.duckdb_decimal_internal_type(logical)),
    })
  else
    -- Scalar leaf: validity already checked above, so pass nil to skip recheck.
    return extract_vector_value(vector, type_id, row, nil)
  end
end

---Render an extracted value (scalar or nested table) to a display string,
---using DuckDB-like syntax: lists as [a, b], structs/maps as {k: v}. Nested
---tables are tagged with `__kind` so NULL elements (nil holes) render safely.
---@param value any
---@return string
local function render_value(value)
  if value == nil then
    return "NULL"
  end
  if type(value) ~= "table" then
    return tostring(value)
  end

  local kind = value.__kind
  if kind == "list" then
    local parts = {}
    for i = 1, value.n do
      parts[i] = render_value(value[i])
    end
    return "[" .. table.concat(parts, ", ") .. "]"
  elseif kind == "map" then
    local parts = {}
    for i = 1, value.n do
      local kv = value[i]
      parts[i] = render_value(kv.key) .. "=" .. render_value(kv.value)
    end
    return "{" .. table.concat(parts, ", ") .. "}"
  elseif kind == "struct" then
    local parts = {}
    for i, field in ipairs(value.fields) do
      parts[i] = string.format("%s: %s", field.name, render_value(field.value))
    end
    return "{" .. table.concat(parts, ", ") .. "}"
  end

  -- Unknown table shape: best-effort positional render.
  return "[" .. tostring(value) .. "]"
end

M._render_value = render_value

-- ============================================================================
-- Connection Management
-- ============================================================================

---Create a new DuckDB connection
---@param path string? File path for a persistent database; nil opens in-memory
---@return DuckDBConnection? connection
---@return string? error
function M.create_connection(path)
  if not duckdb_ffi.lib then
    return nil, "DuckDB library not loaded"
  end

  local db = ffi.new("duckdb_database[1]")
  local conn = ffi.new("duckdb_connection[1]")

  -- Open a persistent file database when a path is given, else in-memory.
  local state = duckdb_ffi.C.duckdb_open(path, db)
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

---Build a classified error message from a populated duckdb_result.
---@param result ffi.cdata* duckdb_result[1] with an error set
---@return string
local function result_error_message(result)
  local error_msg = "Query failed"
  local err_ptr = duckdb_ffi.C.duckdb_result_error(result)
  if err_ptr ~= nil then
    error_msg = ffi.string(err_ptr)
  end
  -- Classify the error by DuckDB's own error category for clearer diagnostics.
  local err_type = tonumber(duckdb_ffi.C.duckdb_result_error_type(result))
  local category = duckdb_ffi.error_type_names[err_type]
  if category and category ~= "INVALID" then
    error_msg = string.format("[%s] %s", category, error_msg)
  end
  return error_msg
end

---Collect a populated duckdb_result into a QueryResult and destroy it.
---Shared by the synchronous and pending (async) execution paths.
---@param result ffi.cdata* duckdb_result[1], already successfully executed
---@return QueryResult
local function collect_result(result)
  -- Extract column information
  local column_count = tonumber(duckdb_ffi.C.duckdb_column_count(result))
  local rows_changed = tonumber(duckdb_ffi.C.duckdb_rows_changed(result))
  -- Authoritative statement kind (SELECT vs DDL/DML). Passed by value.
  local statement_type = tonumber(duckdb_ffi.C.duckdb_result_statement_type(result[0]))

  -- Types that need the logical type for extraction (recursive / dictionary /
  -- scaled). These take the slower logical-type-driven path; everything else
  -- uses the fast raw-buffer cast.
  local complex_types = {
    [T.LIST] = true,
    [T.ARRAY] = true,
    [T.STRUCT] = true,
    [T.MAP] = true,
    [T.ENUM] = true,
    [T.DECIMAL] = true,
  }

  local columns = {}
  local column_types = {}
  local has_complex = false
  for col = 0, column_count - 1 do
    local col_name = duckdb_ffi.C.duckdb_column_name(result, col)
    if col_name ~= nil then
      table.insert(columns, ffi.string(col_name))
    else
      table.insert(columns, string.format("column_%d", col))
    end
    local col_type = tonumber(duckdb_ffi.C.duckdb_column_type(result, col))
    table.insert(column_types, col_type)
    if complex_types[col_type] then
      has_complex = true
    end
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

    -- Cache vectors, validity masks, and (for complex columns) logical types.
    local vectors = {}
    local validities = {}
    local logicals = {}
    for col = 0, column_count - 1 do
      vectors[col] = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, col)
      validities[col] = duckdb_ffi.C.duckdb_vector_get_validity(vectors[col])
      if complex_types[column_types[col + 1]] then
        logicals[col] = duckdb_ffi.C.duckdb_vector_get_column_type(vectors[col])
      end
    end

    -- Extract rows from this chunk
    for row = 0, chunk_size - 1 do
      local row_data = {}
      for col = 0, column_count - 1 do
        local value
        if logicals[col] ~= nil then
          local extracted = extract_value(vectors[col], logicals[col], row, validities[col])
          -- Render nested/enum/decimal values to display strings; a top-level
          -- NULL extracts to nil and becomes the NULL sentinel below.
          if extracted ~= nil then
            value = render_value(extracted)
          end
        else
          value = extract_vector_value(vectors[col], column_types[col + 1], row, validities[col])
        end
        -- Store the NULL sentinel (not Lua nil) so the column slot is preserved:
        -- table.insert/ipairs/# all mishandle nil holes and would misalign rows.
        row_data[col + 1] = value ~= nil and value or M.NULL
      end
      rows[#rows + 1] = row_data
    end

    -- Release per-chunk logical types before fetching the next chunk.
    if has_complex then
      for col = 0, column_count - 1 do
        if logicals[col] ~= nil then
          destroy_logical(logicals[col])
        end
      end
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
    statement_type = statement_type,
  }
end

---Guard a connection before execution. Returns an error string if unusable.
---@param connection DuckDBConnection
---@return string? error
local function check_connection(connection)
  if connection._closed or not connection.conn then
    return "Connection is closed"
  end
  return nil
end

---Execute a query and return results using modern data chunk API.
---Synchronous: blocks until the query completes. For long queries prefer
---execute_query_async, which keeps the Neovim event loop responsive.
---@param connection DuckDBConnection
---@param query string SQL query
---@return QueryResult? result
---@return string? error
function M.execute_query(connection, query)
  local conn_err = check_connection(connection)
  if conn_err then
    return nil, conn_err
  end

  local result = ffi.new("duckdb_result[1]")
  local state = duckdb_ffi.C.duckdb_query(connection.conn[0], query, result)
  if state ~= 0 then
    local error_msg = result_error_message(result)
    duckdb_ffi.C.duckdb_destroy_result(result)
    return nil, error_msg
  end

  return collect_result(result)
end

---Interrupt a query running on this connection (e.g. from execute_query_async).
---Safe to call when no query is running. The interrupted query surfaces as an
---INTERRUPT-category error in its callback.
---@param connection DuckDBConnection
function M.interrupt(connection)
  if connection.conn and not connection._closed then
    duckdb_ffi.C.duckdb_interrupt(connection.conn[0])
  end
end

---Drive an already-prepared statement to completion via the pending-result
---interface, off the main loop. Takes ownership of `prepared` and destroys it
---(along with the pending result) when finished.
---@param connection DuckDBConnection
---@param prepared ffi.cdata* duckdb_prepared_statement[1], owned by this call
---@param callback fun(result: QueryResult?, err: string?) Invoked on completion
---@param interval integer Timer period in ms
---@return boolean started False if the pending result could not be created
local function drive_prepared_async(connection, prepared, callback, interval)
  -- Create a pending result that we drive task-by-task.
  local pending = ffi.new("duckdb_pending_result[1]")
  local state = duckdb_ffi.C.duckdb_pending_prepared(prepared[0], pending)
  if state ~= 0 then
    local err_ptr = duckdb_ffi.C.duckdb_pending_error(pending[0])
    local msg = err_ptr ~= nil and ffi.string(err_ptr) or "Failed to create pending result"
    duckdb_ffi.C.duckdb_destroy_pending(pending)
    duckdb_ffi.C.duckdb_destroy_prepare(prepared)
    callback(nil, msg)
    return false
  end

  local PS = duckdb_ffi.pending_state
  local timer = vim.loop.new_timer()
  local finished = false

  -- Tear down FFI handles exactly once, regardless of success/error/cancel.
  local function cleanup()
    if not timer:is_closing() then
      timer:stop()
      timer:close()
    end
    duckdb_ffi.C.duckdb_destroy_pending(pending)
    duckdb_ffi.C.duckdb_destroy_prepare(prepared)
  end

  -- Resolve once: guard against double-callback (e.g. cancel racing a tick).
  local function finish(result, err)
    if finished then
      return
    end
    finished = true
    cleanup()
    callback(result, err)
  end

  timer:start(0, interval, function()
    if finished then
      return
    end

    -- Run a single execution task. Its RETURN VALUE is the authoritative state:
    -- duckdb_pending_execute_check_state can sit at NO_TASKS_AVAILABLE while the
    -- query is still progressing, so we key off the task return + is_finished.
    local pstate = duckdb_ffi.C.duckdb_pending_execute_task(pending[0])

    if tonumber(pstate) == PS.ERROR then
      local err_ptr = duckdb_ffi.C.duckdb_pending_error(pending[0])
      local msg = err_ptr ~= nil and ffi.string(err_ptr) or "Query failed"
      -- Marshal back onto the main loop before touching Neovim state.
      vim.schedule(function()
        finish(nil, msg)
      end)
    elseif duckdb_ffi.C.duckdb_pending_execution_is_finished(pstate) then
      -- Execution done; materialize the result (synchronous, fast).
      local result = ffi.new("duckdb_result[1]")
      local exec_state = duckdb_ffi.C.duckdb_execute_pending(pending[0], result)
      if exec_state ~= 0 then
        local msg = result_error_message(result)
        duckdb_ffi.C.duckdb_destroy_result(result)
        vim.schedule(function()
          finish(nil, msg)
        end)
      else
        vim.schedule(function()
          if finished then
            duckdb_ffi.C.duckdb_destroy_result(result)
            return
          end
          finish(collect_result(result), nil)
        end)
      end
    end
    -- NOT_READY / NO_TASKS_AVAILABLE: keep ticking.
  end)

  return true
end

---Execute a query without blocking the Neovim event loop.
---
---Drives DuckDB's pending-result interface from a libuv timer: each tick runs
---one execution task, yields back to the loop, and reschedules until the result
---is ready. Result collection (chunk fetch) still happens synchronously once
---execution finishes — that part is fast relative to query planning/scanning.
---
---The query is cancellable mid-flight via M.interrupt(connection).
---
---@param connection DuckDBConnection
---@param query string SQL query
---@param callback fun(result: QueryResult?, err: string?) Invoked on completion
---@param opts? { interval_ms?: integer } interval_ms: timer period (default 5)
---@return fun()|nil cancel A function that interrupts the in-flight query, or nil if setup failed
function M.execute_query_async(connection, query, callback, opts)
  opts = opts or {}
  local interval = opts.interval_ms or 5

  local conn_err = check_connection(connection)
  if conn_err then
    callback(nil, conn_err)
    return nil
  end

  -- Prepare the statement (cheap; surfaces parse/bind errors immediately).
  local prepared = ffi.new("duckdb_prepared_statement[1]")
  local state = duckdb_ffi.C.duckdb_prepare(connection.conn[0], query, prepared)
  if state ~= 0 then
    local err_ptr = duckdb_ffi.C.duckdb_prepare_error(prepared[0])
    local msg = err_ptr ~= nil and ffi.string(err_ptr) or "Failed to prepare statement"
    duckdb_ffi.C.duckdb_destroy_prepare(prepared)
    callback(nil, msg)
    return nil
  end

  if not drive_prepared_async(connection, prepared, callback, interval) then
    return nil
  end

  -- Cancel handle: interrupt the running query. The next tick observes the
  -- resulting error state and resolves via the normal error path.
  return function()
    M.interrupt(connection)
  end
end

---Execute a parameterized query, binding `values` positionally to its
---parameters ($1..$N or ?), without blocking the Neovim event loop.
---
---All values are bound as VARCHAR; DuckDB casts them to each parameter's target
---type (e.g. '5' -> INTEGER). A value of nil (or the M.NULL sentinel) binds SQL
---NULL. The number of values must match the statement's parameter count.
---
---@param connection DuckDBConnection
---@param query string SQL query containing parameters
---@param values table<any> Positional parameter values (1-based)
---@param callback fun(result: QueryResult?, err: string?) Invoked on completion
---@param opts? { interval_ms?: integer }
---@return fun()|nil cancel Interrupts the in-flight query, or nil on setup failure
function M.execute_query_with_params_async(connection, query, values, callback, opts)
  opts = opts or {}
  local interval = opts.interval_ms or 5

  local conn_err = check_connection(connection)
  if conn_err then
    callback(nil, conn_err)
    return nil
  end

  local prepared = ffi.new("duckdb_prepared_statement[1]")
  local state = duckdb_ffi.C.duckdb_prepare(connection.conn[0], query, prepared)
  if state ~= 0 then
    local err_ptr = duckdb_ffi.C.duckdb_prepare_error(prepared[0])
    local msg = err_ptr ~= nil and ffi.string(err_ptr) or "Failed to prepare statement"
    duckdb_ffi.C.duckdb_destroy_prepare(prepared)
    callback(nil, msg)
    return nil
  end

  -- Validate parameter count up front for a clear error.
  local nparams = tonumber(duckdb_ffi.C.duckdb_nparams(prepared[0]))
  if nparams ~= #values then
    duckdb_ffi.C.duckdb_destroy_prepare(prepared)
    callback(nil, string.format("Query has %d parameter(s) but %d value(s) supplied", nparams, #values))
    return nil
  end

  -- Bind each value positionally (parameter indices are 1-based).
  for i = 1, nparams do
    local v = values[i]
    local bind_state
    if v == nil or v == M.NULL then
      bind_state = duckdb_ffi.C.duckdb_bind_null(prepared[0], i)
    else
      bind_state = duckdb_ffi.C.duckdb_bind_varchar(prepared[0], i, tostring(v))
    end
    if bind_state ~= 0 then
      duckdb_ffi.C.duckdb_destroy_prepare(prepared)
      callback(nil, string.format("Failed to bind parameter %d", i))
      return nil
    end
  end

  if not drive_prepared_async(connection, prepared, callback, interval) then
    return nil
  end

  return function()
    M.interrupt(connection)
  end
end

---Execute a multi-statement SQL script without blocking the Neovim event loop.
---
---Splits the script into individual statements via duckdb_extract_statements,
---then prepares and executes each one in order. Statements run sequentially:
---statement i+1 starts only after statement i finishes, so earlier DDL/DML is
---visible to later statements (e.g. CREATE TABLE then SELECT). The callback
---receives the LAST statement's result; if any statement fails, execution stops
---and the error is reported.
---
---Cancellable mid-flight via M.interrupt(connection); the in-progress statement
---surfaces as an INTERRUPT error and the chain stops.
---
---@param connection DuckDBConnection
---@param script string One or more `;`-separated SQL statements
---@param callback fun(result: QueryResult?, err: string?, info: { statement_count: integer, executed: integer }?) Invoked on completion
---@param opts? { interval_ms?: integer }
---@return fun()|nil cancel Interrupts the in-flight statement, or nil on setup failure
function M.execute_script_async(connection, script, callback, opts)
  opts = opts or {}
  local interval = opts.interval_ms or 5

  local conn_err = check_connection(connection)
  if conn_err then
    callback(nil, conn_err)
    return nil
  end

  -- Split the script into statements. Returns the statement count (0 on error).
  local extracted = ffi.new("duckdb_extracted_statements[1]")
  local count = tonumber(duckdb_ffi.C.duckdb_extract_statements(connection.conn[0], script, extracted))
  if count == 0 then
    local err_ptr = duckdb_ffi.C.duckdb_extract_statements_error(extracted[0])
    local msg = err_ptr ~= nil and ffi.string(err_ptr) or "Failed to parse script"
    duckdb_ffi.C.duckdb_destroy_extracted(extracted)
    callback(nil, msg)
    return nil
  end

  -- Run statement `index` (0-based). On success, recurse to the next; on the
  -- last statement, deliver its result. Each statement is prepared from the
  -- extracted set, then driven via the shared pending-result machinery.
  local function run_statement(index)
    local prepared = ffi.new("duckdb_prepared_statement[1]")
    local state = duckdb_ffi.C.duckdb_prepare_extracted_statement(connection.conn[0], extracted[0], index, prepared)
    if state ~= 0 then
      local err_ptr = duckdb_ffi.C.duckdb_prepare_error(prepared[0])
      local msg = err_ptr ~= nil and ffi.string(err_ptr)
        or string.format("Failed to prepare statement %d/%d", index + 1, count)
      duckdb_ffi.C.duckdb_destroy_prepare(prepared)
      duckdb_ffi.C.duckdb_destroy_extracted(extracted)
      callback(nil, msg, { statement_count = count, executed = index })
      return
    end

    drive_prepared_async(connection, prepared, function(result, err)
      if err then
        duckdb_ffi.C.duckdb_destroy_extracted(extracted)
        callback(nil, err, { statement_count = count, executed = index })
        return
      end
      if index + 1 < count then
        -- More statements remain; intermediate results are discarded.
        run_statement(index + 1)
      else
        -- Last statement: deliver its result.
        duckdb_ffi.C.duckdb_destroy_extracted(extracted)
        callback(result, nil, { statement_count = count, executed = count })
      end
    end, interval)
  end

  run_statement(0)

  return function()
    M.interrupt(connection)
  end
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
      string.format("CREATE TEMP TABLE %s AS SELECT * FROM read_csv('%s')", quote_identifier(table_name), temp_path)
  elseif buffer_info.format == "json" then
    -- Write JSON to temp file
    temp_path, file_err = write_temp_file(buffer_info.content, ".json")
    if not temp_path then
      return false, file_err
    end

    -- Track temp file for cleanup
    table.insert(connection.temp_files, temp_path)

    -- Use read_json_auto which handles both arrays and objects
    create_query = string.format(
      "CREATE TEMP TABLE %s AS SELECT * FROM read_json_auto('%s')",
      quote_identifier(table_name),
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
      quote_identifier(table_name),
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

---Load all buffers referenced by a query into a connection and rewrite the
---query to reference the resulting table names. Synchronous; this is local
---file/buffer I/O and is fast relative to query execution.
---@param conn DuckDBConnection
---@param query string SQL query
---@param buffer_identifier string|number|nil Buffer identifier
---@return string processed_query Query with buffer() references substituted
local function prepare_buffer_query(conn, query, buffer_identifier)
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
    local quoted_name = quote_identifier(loaded_tables[i])
    -- Replace buffer('name') or buffer(num) with quoted table name
    if type(id) == "string" then
      processed_query =
        processed_query:gsub("buffer%s*%(%s*['\"]" .. id:gsub("[%-%.]", "%%%1") .. "['\"]%s*%)", quoted_name)
    else
      processed_query = processed_query:gsub("buffer%s*%(%s*" .. id .. "%s*%)", quoted_name)
    end
  end

  -- Replace standalone 'buffer' references if we loaded a single table
  if #loaded_tables == 1 then
    processed_query = processed_query:gsub("%f[%w]buffer%f[%W]", quote_identifier(loaded_tables[1]))
  end

  return processed_query
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
    local processed_query = prepare_buffer_query(conn, query, buffer_identifier)

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

---Execute query on buffer(s) without blocking the Neovim event loop.
---
---Buffer loading is synchronous (fast local I/O); only the query execution is
---driven asynchronously via the pending-result interface. The connection stays
---open until execution completes, then is closed before the callback fires.
---
---@param query string SQL query
---@param buffer_identifier string|number|nil Buffer identifier
---@param callback fun(result: QueryResult?, err: string?) Invoked on completion
---@param opts? { interval_ms?: integer } Forwarded to execute_query_async
---@return DuckDBConnection? connection The live connection (for M.interrupt), or nil on setup failure
function M.query_buffer_async(query, buffer_identifier, callback, opts)
  local conn, err = M.create_connection()
  if not conn then
    callback(nil, err)
    return nil
  end

  -- Buffer loading + query rewrite (synchronous, may raise via error()).
  local ok, processed_query = pcall(prepare_buffer_query, conn, query, buffer_identifier)
  if not ok then
    M.close_connection(conn)
    callback(nil, tostring(processed_query))
    return nil
  end

  M.execute_query_async(conn, processed_query, function(result, query_err)
    M.close_connection(conn)
    callback(result, query_err)
  end, opts)

  return conn
end

---Like query_buffer_async, but the query carries positional parameters bound
---from `values`. Buffer references are loaded/rewritten, then the parameterized
---query runs against them.
---@param query string SQL query with $1..$N / ? parameters
---@param values table<any> Positional parameter values
---@param buffer_identifier string|number|nil
---@param callback fun(result: QueryResult?, err: string?)
---@param opts? { interval_ms?: integer }
---@return DuckDBConnection? connection
function M.query_buffer_with_params_async(query, values, buffer_identifier, callback, opts)
  local conn, err = M.create_connection()
  if not conn then
    callback(nil, err)
    return nil
  end

  local ok, processed_query = pcall(prepare_buffer_query, conn, query, buffer_identifier)
  if not ok then
    M.close_connection(conn)
    callback(nil, tostring(processed_query))
    return nil
  end

  M.execute_query_with_params_async(conn, processed_query, values, function(result, query_err)
    M.close_connection(conn)
    callback(result, query_err)
  end, opts)

  return conn
end

-- ============================================================================
-- Appender (fast bulk insert)
-- ============================================================================

---Bulk-insert rows into an existing table using DuckDB's appender API.
---
---Every cell is appended as VARCHAR (the M.NULL sentinel or Lua nil appends SQL
---NULL); DuckDB casts to each column's declared type. The target table must
---already exist with a column count matching each row's length. This is far
---faster than generating one INSERT per row.
---
---Synchronous: the appender is an in-memory fast path, not a planned query.
---
---@param connection DuckDBConnection
---@param table_name string Existing target table
---@param rows table<table> Array of rows; each row an array of cell values
---@return integer? appended Number of rows appended, or nil on error
---@return string? error
function M.append_rows(connection, table_name, rows)
  local conn_err = check_connection(connection)
  if conn_err then
    return nil, conn_err
  end

  local appender = ffi.new("duckdb_appender[1]")
  -- schema = nil selects the default schema.
  local state = duckdb_ffi.C.duckdb_appender_create(connection.conn[0], nil, table_name, appender)
  if state ~= 0 then
    local err_ptr = duckdb_ffi.C.duckdb_appender_error(appender[0])
    local msg = err_ptr ~= nil and ffi.string(err_ptr) or "Failed to create appender"
    duckdb_ffi.C.duckdb_appender_destroy(appender)
    return nil, msg
  end

  -- Append every cell. On any failure, surface the appender's error and bail.
  local function fail()
    local err_ptr = duckdb_ffi.C.duckdb_appender_error(appender[0])
    local msg = err_ptr ~= nil and ffi.string(err_ptr) or "Append failed"
    duckdb_ffi.C.duckdb_appender_destroy(appender)
    return nil, msg
  end

  for _, row in ipairs(rows) do
    for _, cell in ipairs(row) do
      local s
      if cell == nil or cell == M.NULL then
        s = duckdb_ffi.C.duckdb_append_null(appender[0])
      else
        s = duckdb_ffi.C.duckdb_append_varchar(appender[0], tostring(cell))
      end
      if s ~= 0 then
        return fail()
      end
    end
    if duckdb_ffi.C.duckdb_appender_end_row(appender[0]) ~= 0 then
      return fail()
    end
  end

  -- Flush commits the appended rows; check for constraint/cast errors here too.
  if duckdb_ffi.C.duckdb_appender_flush(appender[0]) ~= 0 then
    return fail()
  end

  duckdb_ffi.C.duckdb_appender_destroy(appender)
  return #rows, nil
end

return M
