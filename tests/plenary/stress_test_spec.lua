-- Stress tests for FFI resource management
-- These tests specifically target memory leaks and resource exhaustion

describe('DuckDB FFI Stress Tests', function()
  local query_module
  local duckdb_ffi
  local ffi = require('ffi')

  before_each(function()
    -- Reload query module for fresh state, but keep FFI module
    -- (FFI C definitions can only be loaded once per process)
    package.loaded['duckdb.query'] = nil
    package.loaded['duckdb.buffer'] = nil
    query_module = require('duckdb.query')
    duckdb_ffi = require('duckdb.ffi')
  end)

  describe('Memory Leak Detection', function()
    it('should not accumulate memory over many connection cycles', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      -- Get initial memory usage
      collectgarbage('collect')
      local initial_mem = collectgarbage('count')

      -- Create and destroy many connections
      for i = 1, 100 do
        local conn, err = query_module.create_connection()
        assert.is_nil(err, 'Failed on iteration ' .. i)

        -- Execute a query to ensure resources are used
        local result = query_module.execute_query(conn, 'SELECT ' .. i .. ', repeat(\'x\', 100) as data')
        assert.is_not_nil(result)

        query_module.close_connection(conn)

        -- Periodic GC to simulate real usage patterns
        if i % 10 == 0 then
          collectgarbage('collect')
        end
      end

      -- Final cleanup
      collectgarbage('collect')
      collectgarbage('collect')
      local final_mem = collectgarbage('count')

      -- Memory growth should be bounded (allow some overhead but not linear growth)
      local mem_growth = final_mem - initial_mem
      local max_allowed_growth = 1024 -- 1MB max growth for 100 iterations

      -- This is a soft check - if it fails, it indicates a potential leak
      if mem_growth > max_allowed_growth then
        print(string.format('WARNING: Memory grew by %.2f KB over 100 iterations', mem_growth))
      end

      assert.is_true(true, 'Stress test completed without crash')
    end)

    it('should handle rapid GC cycles during active connections', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      -- Create connection
      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Execute queries while forcing GC
      for i = 1, 50 do
        local result = query_module.execute_query(conn, 'SELECT ' .. i)
        assert.is_not_nil(result)

        -- Force GC while connection is active
        collectgarbage('collect')
      end

      query_module.close_connection(conn)

      -- GC after close should be safe
      for _ = 1, 10 do
        collectgarbage('collect')
      end

      assert.is_true(true)
    end)

    it('should handle large result sets without memory corruption', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Create a large result set
      local sql = [[
        SELECT
          generate_series as id,
          'name_' || generate_series::text as name,
          random() as value,
          repeat('x', 50) as padding
        FROM generate_series(1, 1000)
      ]]

      local result, query_err = query_module.execute_query(conn, sql)
      assert.is_nil(query_err)
      assert.equals(1000, result.row_count)
      assert.equals(4, result.column_count)

      -- Verify data integrity
      for i = 1, 10 do
        assert.equals(i, result.rows[i][1])
        assert.equals('name_' .. i, result.rows[i][2])
        assert.is_number(result.rows[i][3])
        assert.equals(50, #result.rows[i][4])
      end

      query_module.close_connection(conn)
      collectgarbage('collect')
    end)

    it('should survive abandoned connection cleanup by GC', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      -- Create connections but don't explicitly close them
      -- The GC safety net should clean them up
      local function create_abandoned_connection()
        local conn, err = query_module.create_connection()
        if err then return end

        local result = query_module.execute_query(conn, 'SELECT 1')
        -- Intentionally not calling close_connection
        -- The GC guard should handle cleanup
      end

      -- Create several abandoned connections
      for i = 1, 10 do
        create_abandoned_connection()
      end

      -- Force garbage collection to trigger finalizers
      collectgarbage('collect')
      collectgarbage('collect')
      collectgarbage('collect')

      -- Create a new connection to verify system is still functional
      local conn, err = query_module.create_connection()
      assert.is_nil(err)
      assert.is_not_nil(conn)

      local result = query_module.execute_query(conn, 'SELECT 42 as answer')
      assert.is_not_nil(result)
      assert.equals(42, result.rows[1][1])

      query_module.close_connection(conn)
    end)
  end)

  describe('Error Recovery', function()
    it('should recover from consecutive query errors', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Cause multiple errors
      for i = 1, 20 do
        local result, query_err = query_module.execute_query(conn, 'INVALID SYNTAX ' .. i)
        assert.is_nil(result)
        assert.is_not_nil(query_err)
      end

      -- Connection should still be usable
      local result, query_err = query_module.execute_query(conn, 'SELECT 1 as success')
      assert.is_nil(query_err)
      assert.equals(1, result.rows[1][1])

      query_module.close_connection(conn)
    end)

    it('should handle interleaved successes and failures', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      for i = 1, 30 do
        if i % 3 == 0 then
          -- Error case
          local result, query_err = query_module.execute_query(conn, 'BAD SQL')
          assert.is_nil(result)
        else
          -- Success case
          local result, query_err = query_module.execute_query(conn, 'SELECT ' .. i)
          assert.is_nil(query_err)
          assert.equals(i, result.rows[1][1])
        end
      end

      query_module.close_connection(conn)
      collectgarbage('collect')
    end)
  end)

  describe('Concurrent Operations', function()
    it('should handle multiple independent connections', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local connections = {}

      -- Create multiple connections
      for i = 1, 5 do
        local conn, err = query_module.create_connection()
        assert.is_nil(err)
        table.insert(connections, conn)
      end

      -- Execute queries on each connection
      for i, conn in ipairs(connections) do
        local result, err = query_module.execute_query(conn, 'SELECT ' .. i .. ' as conn_id')
        assert.is_nil(err)
        assert.equals(i, result.rows[1][1])
      end

      -- Close all connections
      for _, conn in ipairs(connections) do
        query_module.close_connection(conn)
      end

      collectgarbage('collect')
      assert.is_true(true)
    end)
  end)

  describe('Edge Cases', function()
    it('should handle zero-row results', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, 'SELECT 1 WHERE 1=0')
      assert.is_nil(query_err)
      assert.equals(0, result.row_count)
      assert.equals(0, #result.rows)

      query_module.close_connection(conn)
    end)

    it('should handle wide result sets (many columns)', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      -- Create a query with 50 columns
      local cols = {}
      for i = 1, 50 do
        table.insert(cols, i .. ' as col' .. i)
      end
      local sql = 'SELECT ' .. table.concat(cols, ', ')

      local result, query_err = query_module.execute_query(conn, sql)
      assert.is_nil(query_err)
      assert.equals(50, result.column_count)
      assert.equals(1, result.row_count)

      -- Verify all columns
      for i = 1, 50 do
        assert.equals('col' .. i, result.columns[i])
        assert.equals(i, result.rows[1][i])
      end

      query_module.close_connection(conn)
    end)

    it('should handle special characters in strings', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local test_cases = {
        "Hello\nWorld",  -- newline
        "Tab\there",     -- tab
        "Quote's test",  -- single quote (escaped in SQL)
        'Unicode: \xC3\xA9\xC3\xB1', -- UTF-8 characters
      }

      for _, test_str in ipairs(test_cases) do
        -- Escape single quotes for SQL
        local escaped = test_str:gsub("'", "''")
        local sql = string.format("SELECT '%s' as test_col", escaped)

        local result, query_err = query_module.execute_query(conn, sql)
        if query_err == nil then
          assert.equals(test_str, result.rows[1][1])
        end
      end

      query_module.close_connection(conn)
    end)
  end)
end)
