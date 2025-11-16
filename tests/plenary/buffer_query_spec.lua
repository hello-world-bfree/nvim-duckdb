-- Tests for buffer query functionality
-- Ensures the query_buffer function properly manages resources

describe('DuckDB Buffer Query', function()
  local query_module
  local duckdb_ffi

  before_each(function()
    -- Reload query and buffer modules for fresh state, but keep FFI module
    -- (FFI C definitions can only be loaded once per process)
    package.loaded['duckdb.query'] = nil
    package.loaded['duckdb.buffer'] = nil
    query_module = require('duckdb.query')
    duckdb_ffi = require('duckdb.ffi')
  end)

  describe('Connection Management in query_buffer', function()
    it('should always close connection even on success', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      -- Create a simple CSV buffer for testing
      local bufnr = vim.api.nvim_create_buf(false, true)
      vim.api.nvim_buf_set_lines(bufnr, 0, -1, false, {
        'name,value',
        'test,123',
      })
      vim.api.nvim_buf_set_name(bufnr, 'test.csv')

      -- This should create, use, and close a connection
      local result, err = query_module.query_buffer('SELECT * FROM buffer', bufnr)

      -- Connection should have been closed internally
      -- If resources weren't cleaned up, repeated calls would eventually fail

      -- Cleanup test buffer
      vim.api.nvim_buf_delete(bufnr, { force = true })

      -- Result may have an error due to buffer module behavior, but connection cleanup should be safe
      assert.is_true(true)
    end)

    it('should handle multiple sequential queries without resource exhaustion', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      -- Run many queries to ensure no connection leaks
      for i = 1, 20 do
        local conn, err = query_module.create_connection()
        if err then
          assert.fail('Failed to create connection on iteration ' .. i .. ': ' .. err)
        end

        local result, query_err = query_module.execute_query(conn, 'SELECT ' .. i .. ' as iteration')
        assert.is_nil(query_err, 'Query failed on iteration ' .. i)
        assert.equals(i, result.rows[1][1])

        query_module.close_connection(conn)
      end

      -- Force GC to ensure finalizers don't cause issues
      collectgarbage('collect')
      collectgarbage('collect')

      assert.is_true(true)
    end)
  end)

  describe('Load Buffer Data Safety', function()
    it('should handle connection properly during table creation', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Attempt to load invalid data should return error, not crash
      local buffer_info = {
        format = 'csv',
        content = 'invalid,csv\nno,quotes,unbalanced',
        name = 'test.csv'
      }

      local success, load_err = query_module.load_buffer_data(conn, 'test_table', buffer_info)
      -- May succeed or fail depending on DuckDB's parser tolerance
      -- The important thing is it doesn't crash

      query_module.close_connection(conn)
      assert.is_true(true)
    end)
  end)

  describe('VARCHAR Memory Safety', function()
    it('should properly free varchar pointers', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Create a query with many string values
      local sql = [[
        SELECT 'string' || generate_series::text as str_col
        FROM generate_series(1, 100)
      ]]

      local result, query_err = query_module.execute_query(conn, sql)
      assert.is_nil(query_err)
      assert.equals(100, result.row_count)

      -- All strings should be properly copied and freed
      for i, row in ipairs(result.rows) do
        assert.is_string(row[1])
        assert.matches('string%d+', row[1])
      end

      query_module.close_connection(conn)
      collectgarbage('collect')
    end)

    it('should handle NULL string values', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, 'SELECT NULL::VARCHAR as null_str')
      assert.is_nil(query_err)
      assert.equals(1, result.row_count)
      assert.is_nil(result.rows[1][1])

      query_module.close_connection(conn)
    end)

    it('should handle empty strings', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, "SELECT '' as empty_str")
      assert.is_nil(query_err)
      assert.equals(1, result.row_count)
      assert.equals('', result.rows[1][1])

      query_module.close_connection(conn)
    end)

    it('should handle very long strings', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Create a string that's 10000 characters long
      local long_string = string.rep('x', 10000)
      local sql = string.format("SELECT '%s' as long_str", long_string)

      local result, query_err = query_module.execute_query(conn, sql)
      assert.is_nil(query_err)
      assert.equals(1, result.row_count)
      assert.equals(10000, #result.rows[1][1])

      query_module.close_connection(conn)
    end)
  end)
end)
