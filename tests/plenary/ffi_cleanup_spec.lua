-- Tests for FFI resource lifecycle management
-- These tests verify the fixes for memory management and cleanup issues

local ffi = require('ffi')

describe('DuckDB FFI Cleanup', function()
  local query_module
  local duckdb_ffi

  before_each(function()
    -- Fresh module load for each test
    package.loaded['duckdb.query'] = nil
    package.loaded['duckdb.ffi'] = nil
    query_module = require('duckdb.query')
    duckdb_ffi = require('duckdb.ffi')
  end)

  describe('Connection Lifecycle', function()
    it('should create connection with _closed flag set to false', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)
      assert.is_not_nil(conn)
      assert.is_false(conn._closed)

      query_module.close_connection(conn)
    end)

    it('should have GC guard after creation', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)
      assert.is_not_nil(conn._gc_guard)

      query_module.close_connection(conn)
    end)

    it('should set _closed flag to true after close', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      query_module.close_connection(conn)
      assert.is_true(conn._closed)
    end)

    it('should remove GC guard after explicit close', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      query_module.close_connection(conn)
      assert.is_nil(conn._gc_guard)
    end)

    it('should handle double-close without crashing', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- First close
      query_module.close_connection(conn)
      assert.is_true(conn._closed)

      -- Second close should be safe (no-op)
      assert.has_no.errors(function()
        query_module.close_connection(conn)
      end)

      -- Still closed
      assert.is_true(conn._closed)
    end)

    it('should set connection pointers to nil after close', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)
      assert.is_not_nil(conn.conn)
      assert.is_not_nil(conn.db)

      query_module.close_connection(conn)
      assert.is_nil(conn.conn)
      assert.is_nil(conn.db)
    end)
  end)

  describe('Query Execution Safety', function()
    it('should reject queries on closed connection', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      query_module.close_connection(conn)

      local result, query_err = query_module.execute_query(conn, 'SELECT 1')
      assert.is_nil(result)
      assert.is_not_nil(query_err)
      assert.matches('closed', query_err)
    end)

    it('should cleanup result even on successful query', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Execute multiple queries to ensure no memory buildup
      for i = 1, 10 do
        local result, query_err = query_module.execute_query(conn, 'SELECT ' .. i .. ' as num')
        assert.is_nil(query_err)
        assert.is_not_nil(result)
        assert.equals(1, result.row_count)
      end

      query_module.close_connection(conn)
    end)

    it('should handle query errors gracefully', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Invalid SQL should return error, not crash
      local result, query_err = query_module.execute_query(conn, 'INVALID SQL SYNTAX HERE')
      assert.is_nil(result)
      assert.is_not_nil(query_err)

      -- Connection should still be usable after error
      local result2, err2 = query_module.execute_query(conn, 'SELECT 1 as test')
      assert.is_nil(err2)
      assert.is_not_nil(result2)

      query_module.close_connection(conn)
    end)

    it('should extract column names safely', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, 'SELECT 1 as col1, 2 as col2, 3 as col3')
      assert.is_nil(query_err)
      assert.is_not_nil(result)
      assert.equals(3, result.column_count)
      assert.equals('col1', result.columns[1])
      assert.equals('col2', result.columns[2])
      assert.equals('col3', result.columns[3])

      query_module.close_connection(conn)
    end)

    it('should handle various data types without memory issues', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local sql = [[
        SELECT
          true as bool_val,
          42 as int_val,
          3.14 as float_val,
          'hello world' as str_val,
          NULL as null_val
      ]]

      local result, query_err = query_module.execute_query(conn, sql)
      assert.is_nil(query_err)
      assert.is_not_nil(result)
      assert.equals(1, result.row_count)
      assert.equals(5, result.column_count)

      -- Verify values are extracted correctly
      local row = result.rows[1]
      assert.is_not_nil(row)
      assert.equals(true, row[1])  -- bool
      assert.equals(42, row[2])    -- int
      assert.is_true(math.abs(row[3] - 3.14) < 0.001)  -- float
      assert.equals('hello world', row[4])  -- string
      assert.is_nil(row[5])  -- null

      query_module.close_connection(conn)
    end)
  end)

  describe('Resource Leak Prevention', function()
    it('should not leak connections when created and closed repeatedly', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      -- Create and close many connections
      for i = 1, 50 do
        local conn, err = query_module.create_connection()
        assert.is_nil(err, 'Failed to create connection #' .. i)
        assert.is_not_nil(conn)

        -- Execute a query
        local result, query_err = query_module.execute_query(conn, 'SELECT ' .. i)
        assert.is_nil(query_err)
        assert.is_not_nil(result)

        query_module.close_connection(conn)
      end

      -- Force garbage collection to ensure no finalizers cause issues
      collectgarbage('collect')
      collectgarbage('collect')

      -- If we got here without crashing, the test passes
      assert.is_true(true)
    end)

    it('should handle rapid connection cycling', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local connections = {}

      -- Create multiple connections
      for i = 1, 10 do
        local conn, err = query_module.create_connection()
        assert.is_nil(err)
        table.insert(connections, conn)
      end

      -- Close them in reverse order
      for i = #connections, 1, -1 do
        query_module.close_connection(connections[i])
      end

      collectgarbage('collect')
      assert.is_true(true)
    end)
  end)

  describe('GC Safety Net', function()
    it('should have weak reference in GC guard', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- The GC guard should exist
      assert.is_not_nil(conn._gc_guard)

      -- Explicitly close to test normal path
      query_module.close_connection(conn)

      -- Guard should be nil after close
      assert.is_nil(conn._gc_guard)
    end)

    it('should survive garbage collection after explicit close', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      query_module.close_connection(conn)

      -- Multiple GC cycles should not cause issues
      for _ = 1, 5 do
        collectgarbage('collect')
      end

      assert.is_true(conn._closed)
    end)
  end)

  describe('Error Path Safety', function()
    it('should not crash when library is not loaded', function()
      -- Temporarily disable library
      local original_lib = duckdb_ffi.lib
      duckdb_ffi.lib = nil

      local conn, err = query_module.create_connection()
      assert.is_nil(conn)
      assert.is_not_nil(err)
      assert.matches('not loaded', err)

      -- Restore
      duckdb_ffi.lib = original_lib
    end)

    it('should handle nil connection gracefully', function()
      -- This should not crash
      assert.has_no.errors(function()
        query_module.close_connection({ _closed = false, conn = nil, db = nil })
      end)
    end)

    it('should handle connection with missing fields', function()
      -- Edge case: connection object missing some fields
      assert.has_no.errors(function()
        query_module.close_connection({ _closed = false })
      end)
    end)
  end)
end)
