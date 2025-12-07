-- Stress tests for DuckDB FFI bindings
-- Tests connection lifecycle, memory management, and cleanup under load
-- Updated for modern data chunk/vector API (Nov 2025)

local ffi = require('ffi')

describe("duckdb stress tests", function()
  local duckdb_ffi

  before_each(function()
    -- Load FFI module (C definitions are global, only loaded once)
    duckdb_ffi = require('duckdb.ffi')
  end)

  it("handles rapid connection cycling", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    -- Create and destroy connections rapidly
    for i = 1, 50 do
      local db = ffi.new("duckdb_database[1]")
      local conn = ffi.new("duckdb_connection[1]")

      local state = duckdb_ffi.C.duckdb_open(nil, db)
      assert.are.equal(0, state, "Failed to open database on iteration " .. i)

      state = duckdb_ffi.C.duckdb_connect(db[0], conn)
      assert.are.equal(0, state, "Failed to connect on iteration " .. i)

      -- Disconnect before close (correct order)
      duckdb_ffi.C.duckdb_disconnect(conn)
      duckdb_ffi.C.duckdb_close(db)
    end
  end)

  it("handles many queries on single connection", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    -- Execute many queries
    for i = 1, 100 do
      local result = ffi.new("duckdb_result[1]")
      local query = string.format("SELECT %d as num, 'test_%d' as str", i, i)

      state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
      assert.are.equal(0, state, "Query failed on iteration " .. i)

      -- Verify result metadata
      local row_count = tonumber(duckdb_ffi.C.duckdb_row_count(result))
      assert.are.equal(1, row_count)

      local col_count = tonumber(duckdb_ffi.C.duckdb_column_count(result))
      assert.are.equal(2, col_count)

      -- Clean up result
      duckdb_ffi.C.duckdb_destroy_result(result)
    end

    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles large result sets with data chunks", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    -- Create table with many rows using recursive CTE
    local result = ffi.new("duckdb_result[1]")
    local create_query = [[
      WITH RECURSIVE nums AS (
        SELECT 1 as n
        UNION ALL
        SELECT n + 1 FROM nums WHERE n < 1000
      )
      SELECT n, n * 2 as doubled, n * n as squared FROM nums
    ]]

    state = duckdb_ffi.C.duckdb_query(conn[0], create_query, result)
    assert.are.equal(0, state, "Failed to create large result set")

    local row_count = tonumber(duckdb_ffi.C.duckdb_row_count(result))
    assert.are.equal(1000, row_count)

    -- Read values using data chunk API
    local total_rows = 0
    while true do
      local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
      if chunk == nil then
        break
      end

      local chunk_size = tonumber(duckdb_ffi.C.duckdb_data_chunk_get_size(chunk))
      total_rows = total_rows + chunk_size

      -- Get vectors for each column
      local vector = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 0)
      local data = duckdb_ffi.C.duckdb_vector_get_data(vector)
      local int64_data = ffi.cast("int64_t*", data)

      -- Verify first few values in each chunk
      if total_rows <= 10 then
        for row = 0, math.min(chunk_size - 1, 9 - (total_rows - chunk_size)) do
          local expected = (total_rows - chunk_size) + row + 1
          assert.are.equal(expected, tonumber(int64_data[row]))
        end
      end

      -- Destroy chunk
      local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
      duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)
    end

    assert.are.equal(1000, total_rows)

    duckdb_ffi.C.duckdb_destroy_result(result)
    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles inline strings (<=12 bytes)", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    local result = ffi.new("duckdb_result[1]")
    -- Test strings of various lengths (all <= 12 bytes for inline storage)
    local query = "SELECT 'a' as s1, 'hello' as s2, 'twelve chars' as s3"

    state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
    assert.are.equal(0, state)

    -- Fetch chunk
    local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
    assert.is_not_nil(chunk)

    local chunk_size = tonumber(duckdb_ffi.C.duckdb_data_chunk_get_size(chunk))
    assert.are.equal(1, chunk_size)

    -- Extract strings using helper
    local expected = { 'a', 'hello', 'twelve chars' }
    for col = 0, 2 do
      local vector = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, col)
      local data = duckdb_ffi.C.duckdb_vector_get_data(vector)
      local str_data = ffi.cast("duckdb_string_t*", data)
      local str_value = duckdb_ffi.extract_string(str_data)
      assert.are.equal(expected[col + 1], str_value)
    end

    local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
    duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)

    duckdb_ffi.C.duckdb_destroy_result(result)
    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles pointer-based strings (>12 bytes)", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    local result = ffi.new("duckdb_result[1]")
    -- Test strings > 12 bytes (pointer-based storage)
    local long_str = "this is a longer string that exceeds twelve bytes"
    local query = string.format("SELECT '%s' as long_str", long_str)

    state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
    assert.are.equal(0, state)

    -- Fetch chunk
    local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
    assert.is_not_nil(chunk)

    local vector = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 0)
    local data = duckdb_ffi.C.duckdb_vector_get_data(vector)
    local str_data = ffi.cast("duckdb_string_t*", data)
    local str_value = duckdb_ffi.extract_string(str_data)

    assert.are.equal(long_str, str_value)
    assert.is_true(#str_value > 12)

    local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
    duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)

    duckdb_ffi.C.duckdb_destroy_result(result)
    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles many string values with proper chunk cleanup", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    local result = ffi.new("duckdb_result[1]")
    local query = [[
      SELECT 'string' || generate_series::text as str_col
      FROM generate_series(1, 100)
    ]]

    state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
    assert.are.equal(0, state)

    -- Extract all strings via chunks
    local strings = {}
    while true do
      local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
      if chunk == nil then
        break
      end

      local chunk_size = tonumber(duckdb_ffi.C.duckdb_data_chunk_get_size(chunk))
      local vector = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 0)
      local data = duckdb_ffi.C.duckdb_vector_get_data(vector)
      local str_data = ffi.cast("duckdb_string_t*", data)

      for row = 0, chunk_size - 1 do
        local str = duckdb_ffi.extract_string(str_data + row)
        table.insert(strings, str)
      end

      local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
      duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)
    end

    assert.are.equal(100, #strings)
    for i, str in ipairs(strings) do
      assert.matches("string%d+", str)
    end

    duckdb_ffi.C.duckdb_destroy_result(result)
    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles validity masks for NULL values", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    local result = ffi.new("duckdb_result[1]")
    local query = [[
      SELECT *
      FROM (VALUES (1, 'a'), (NULL, 'b'), (3, NULL), (NULL, NULL)) AS t(num, str)
    ]]

    state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
    assert.are.equal(0, state)

    local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
    assert.is_not_nil(chunk)

    local chunk_size = tonumber(duckdb_ffi.C.duckdb_data_chunk_get_size(chunk))
    assert.are.equal(4, chunk_size)

    -- Check validity masks for both columns
    local vec_num = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 0)
    local vec_str = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 1)

    local validity_num = duckdb_ffi.C.duckdb_vector_get_validity(vec_num)
    local validity_str = duckdb_ffi.C.duckdb_vector_get_validity(vec_str)

    -- Row 0: (1, 'a') - both valid
    assert.is_true(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_num, 0))
    assert.is_true(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_str, 0))

    -- Row 1: (NULL, 'b') - num is NULL
    assert.is_false(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_num, 1))
    assert.is_true(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_str, 1))

    -- Row 2: (3, NULL) - str is NULL
    assert.is_true(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_num, 2))
    assert.is_false(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_str, 2))

    -- Row 3: (NULL, NULL) - both NULL
    assert.is_false(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_num, 3))
    assert.is_false(duckdb_ffi.C.duckdb_validity_row_is_valid(validity_str, 3))

    local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
    duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)

    duckdb_ffi.C.duckdb_destroy_result(result)
    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles multiple concurrent results", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    -- Create multiple results before destroying any
    local results = {}
    for i = 1, 10 do
      local result = ffi.new("duckdb_result[1]")
      local query = string.format("SELECT %d as val", i)

      state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
      assert.are.equal(0, state)

      table.insert(results, result)
    end

    -- Verify all results are still valid using chunk API
    for i, result in ipairs(results) do
      local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
      assert.is_not_nil(chunk)

      local vector = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 0)
      local data = duckdb_ffi.C.duckdb_vector_get_data(vector)
      local int32_data = ffi.cast("int32_t*", data)
      assert.are.equal(i, tonumber(int32_data[0]))

      local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
      duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)
    end

    -- Clean up all results
    for _, result in ipairs(results) do
      duckdb_ffi.C.duckdb_destroy_result(result)
    end

    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles error queries gracefully", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    -- Execute invalid query
    local result = ffi.new("duckdb_result[1]")
    state = duckdb_ffi.C.duckdb_query(conn[0], "SELECT * FROM nonexistent_table", result)
    assert.are.equal(1, state, "Invalid query should fail")

    -- Check error message using accessor function
    local err_ptr = duckdb_ffi.C.duckdb_result_error(result)
    assert.is_not_nil(err_ptr)
    local err_msg = ffi.string(err_ptr)
    assert.is_truthy(#err_msg > 0)

    -- Must still destroy result even on error
    duckdb_ffi.C.duckdb_destroy_result(result)

    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles DDL and DML operations with chunk API", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    -- Create table
    local result = ffi.new("duckdb_result[1]")
    state = duckdb_ffi.C.duckdb_query(conn[0], "CREATE TABLE test_table (id INTEGER, name VARCHAR)", result)
    assert.are.equal(0, state)
    duckdb_ffi.C.duckdb_destroy_result(result)

    -- Insert rows
    for i = 1, 20 do
      result = ffi.new("duckdb_result[1]")
      local insert_query = string.format("INSERT INTO test_table VALUES (%d, 'name_%d')", i, i)
      state = duckdb_ffi.C.duckdb_query(conn[0], insert_query, result)
      assert.are.equal(0, state)

      local rows_changed = tonumber(duckdb_ffi.C.duckdb_rows_changed(result))
      assert.are.equal(1, rows_changed)

      duckdb_ffi.C.duckdb_destroy_result(result)
    end

    -- Query all rows and verify count using chunk API
    result = ffi.new("duckdb_result[1]")
    state = duckdb_ffi.C.duckdb_query(conn[0], "SELECT COUNT(*) FROM test_table", result)
    assert.are.equal(0, state)

    local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
    assert.is_not_nil(chunk)

    local vector = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 0)
    local data = duckdb_ffi.C.duckdb_vector_get_data(vector)
    local int64_data = ffi.cast("int64_t*", data)
    assert.are.equal(20, tonumber(int64_data[0]))

    local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
    duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)
    duckdb_ffi.C.duckdb_destroy_result(result)

    -- Drop table
    result = ffi.new("duckdb_result[1]")
    state = duckdb_ffi.C.duckdb_query(conn[0], "DROP TABLE test_table", result)
    assert.are.equal(0, state)
    duckdb_ffi.C.duckdb_destroy_result(result)

    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles chunk destruction safety", function()
    if not duckdb_ffi.lib then
      pending("DuckDB library not available")
      return
    end

    local db = ffi.new("duckdb_database[1]")
    local conn = ffi.new("duckdb_connection[1]")

    local state = duckdb_ffi.C.duckdb_open(nil, db)
    assert.are.equal(0, state)

    state = duckdb_ffi.C.duckdb_connect(db[0], conn)
    assert.are.equal(0, state)

    -- Create a query that spans multiple chunks (DuckDB default chunk size is ~2048)
    local result = ffi.new("duckdb_result[1]")
    local query = "SELECT * FROM generate_series(1, 5000)"

    state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
    assert.are.equal(0, state)

    local chunk_count = 0
    local total_rows = 0

    -- Fetch and destroy all chunks
    while true do
      local chunk = duckdb_ffi.C.duckdb_fetch_chunk(result[0])
      if chunk == nil then
        break
      end

      chunk_count = chunk_count + 1
      local chunk_size = tonumber(duckdb_ffi.C.duckdb_data_chunk_get_size(chunk))
      total_rows = total_rows + chunk_size

      -- Verify we can access data before destruction
      local vector = duckdb_ffi.C.duckdb_data_chunk_get_vector(chunk, 0)
      assert.is_not_nil(vector)

      -- Destroy chunk
      local chunk_ptr = ffi.new("duckdb_data_chunk[1]", chunk)
      duckdb_ffi.C.duckdb_destroy_data_chunk(chunk_ptr)
    end

    assert.are.equal(5000, total_rows)
    assert.is_true(chunk_count > 1, "Expected multiple chunks for large result set")

    duckdb_ffi.C.duckdb_destroy_result(result)
    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)

    -- Force GC to ensure no issues with destroyed chunks
    collectgarbage('collect')
    collectgarbage('collect')
  end)
end)
