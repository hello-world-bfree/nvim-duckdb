-- Stress tests for DuckDB FFI bindings
-- Tests connection lifecycle, memory management, and cleanup under load

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

      -- Verify result
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

  it("handles large result sets", function()
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

    -- Read some values to ensure data is accessible
    for i = 0, 9 do
      local val = duckdb_ffi.C.duckdb_value_int64(result, 0, i)
      assert.are.equal(i + 1, tonumber(val))
    end

    duckdb_ffi.C.duckdb_destroy_result(result)
    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)

  it("handles string value extraction with proper cleanup", function()
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
    local query = "SELECT 'hello' as a, 'world' as b, 'test' as c"

    state = duckdb_ffi.C.duckdb_query(conn[0], query, result)
    assert.are.equal(0, state)

    -- Extract strings and free them properly
    for col = 0, 2 do
      local str_ptr = duckdb_ffi.C.duckdb_value_varchar(result, col, 0)
      assert.is_not_nil(str_ptr)
      local str = ffi.string(str_ptr)
      assert.is_truthy(#str > 0)
      -- Must free the string
      duckdb_ffi.C.duckdb_free(str_ptr)
    end

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

    -- Verify all results are still valid
    for i, result in ipairs(results) do
      local val = duckdb_ffi.C.duckdb_value_int64(result, 0, 0)
      assert.are.equal(i, tonumber(val))
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

  it("handles DDL and DML operations", function()
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

    -- Query all rows
    result = ffi.new("duckdb_result[1]")
    state = duckdb_ffi.C.duckdb_query(conn[0], "SELECT COUNT(*) FROM test_table", result)
    assert.are.equal(0, state)

    local count = tonumber(duckdb_ffi.C.duckdb_value_int64(result, 0, 0))
    assert.are.equal(20, count)

    duckdb_ffi.C.duckdb_destroy_result(result)

    -- Drop table
    result = ffi.new("duckdb_result[1]")
    state = duckdb_ffi.C.duckdb_query(conn[0], "DROP TABLE test_table", result)
    assert.are.equal(0, state)
    duckdb_ffi.C.duckdb_destroy_result(result)

    duckdb_ffi.C.duckdb_disconnect(conn)
    duckdb_ffi.C.duckdb_close(db)
  end)
end)
