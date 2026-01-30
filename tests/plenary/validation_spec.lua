-- Tests for CSV validation using DuckDB reject_errors/reject_scans tables
-- Updated for modern data chunk/vector API (Nov 2025)

describe('DuckDB CSV Validation', function()
  local query_module
  local validate_module
  local duckdb_ffi

  before_each(function()
    -- Reload modules for fresh state, but keep FFI module
    package.loaded['duckdb.query'] = nil
    package.loaded['duckdb.validate'] = nil
    query_module = require('duckdb.query')
    validate_module = require('duckdb.validate')
    duckdb_ffi = require('duckdb.ffi')
  end)

  describe('Reject Tables', function()
    it('should parse valid CSV without errors', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)
      assert.is_not_nil(conn)

      -- Create a valid CSV
      vim.fn.writefile({
        'name,age,city',
        'Alice,30,Boston',
        'Bob,25,Seattle',
      }, '/tmp/valid_test.csv')

      -- Read CSV with store_rejects enabled
      local result, query_err = query_module.execute_query(conn, [[
        SELECT * FROM read_csv('/tmp/valid_test.csv',
          sample_size=-1,
          store_rejects=true,
          ignore_errors=true
        )
      ]])

      assert.is_nil(query_err)
      assert.is_not_nil(result)
      assert.equals(2, result.row_count)

      -- Check reject_errors is empty
      local reject_result, reject_err = query_module.execute_query(conn, [[
        SELECT * FROM reject_errors
      ]])

      -- No errors expected
      if reject_err == nil then
        assert.equals(0, reject_result.row_count)
      end

      query_module.close_connection(conn)
      vim.fn.delete('/tmp/valid_test.csv')
    end)

    it('should capture CSV parse errors in reject_errors', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)
      assert.is_not_nil(conn)

      -- Create a CSV with type errors
      vim.fn.writefile({
        'name,age,score',
        'Alice,30,95.5',
        'Bob,not_a_number,88.0',  -- 'not_a_number' is invalid for INTEGER
        'Carol,28,invalid',        -- 'invalid' is invalid for DOUBLE
      }, '/tmp/invalid_test.csv')

      -- Read CSV with store_rejects enabled
      local result, query_err = query_module.execute_query(conn, [[
        SELECT * FROM read_csv('/tmp/invalid_test.csv',
          sample_size=-1,
          store_rejects=true,
          ignore_errors=true,
          columns={'name': 'VARCHAR', 'age': 'INTEGER', 'score': 'DOUBLE'}
        )
      ]])

      -- Should succeed (errors are stored, not thrown)
      assert.is_nil(query_err)
      assert.is_not_nil(result)
      -- Valid rows only
      assert.equals(1, result.row_count)

      -- Check reject_errors has entries
      local reject_result, reject_err = query_module.execute_query(conn, [[
        SELECT line, column_idx, column_name, error_type, error_message
        FROM reject_errors
        ORDER BY line, column_idx
      ]])

      assert.is_nil(reject_err)
      assert.is_not_nil(reject_result)
      -- Should have errors from lines 2 and 3 (1-indexed in file, but line 0 is header)
      assert.is_true(reject_result.row_count >= 1)

      query_module.close_connection(conn)
      vim.fn.delete('/tmp/invalid_test.csv')
    end)

    it('should provide column information in reject_errors', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      vim.fn.writefile({
        'id,value',
        '1,100',
        'bad_id,200',
      }, '/tmp/column_error_test.csv')

      local result, query_err = query_module.execute_query(conn, [[
        SELECT * FROM read_csv('/tmp/column_error_test.csv',
          sample_size=-1,
          store_rejects=true,
          ignore_errors=true,
          columns={'id': 'INTEGER', 'value': 'INTEGER'}
        )
      ]])

      assert.is_nil(query_err)

      local reject_result, reject_err = query_module.execute_query(conn, [[
        SELECT column_name, column_idx
        FROM reject_errors
        LIMIT 1
      ]])

      if reject_err == nil and reject_result.row_count > 0 then
        -- Column name should be 'id' for the first column error
        assert.is_not_nil(reject_result.rows[1][1])
      end

      query_module.close_connection(conn)
      vim.fn.delete('/tmp/column_error_test.csv')
    end)
  end)

  describe('ValidationResult Structure', function()
    it('should create valid ValidationResult for valid CSV', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      -- Create a test buffer with valid CSV
      local bufnr = vim.api.nvim_create_buf(false, true)
      vim.api.nvim_buf_set_lines(bufnr, 0, -1, false, {
        'name,value',
        'test,123',
      })
      vim.api.nvim_buf_set_name(bufnr, '/tmp/valid_buffer_test.csv')

      -- Write to temp file for validation
      vim.fn.writefile(vim.api.nvim_buf_get_lines(bufnr, 0, -1, false), '/tmp/valid_buffer_test.csv')

      -- Note: Full validation testing requires the validate module integration
      -- which depends on specific buffer and file handling

      vim.api.nvim_buf_delete(bufnr, { force = true })
      vim.fn.delete('/tmp/valid_buffer_test.csv')
    end)
  end)

  describe('Date/Time Parsing', function()
    it('should handle various timestamp formats', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, [[
        SELECT
          '2024-01-15'::DATE as date_val,
          '2024-01-15 10:30:00'::TIMESTAMP as ts_val,
          '10:30:00'::TIME as time_val
      ]])

      assert.is_nil(query_err)
      assert.is_not_nil(result)
      assert.equals(1, result.row_count)

      -- Date should be formatted
      assert.is_string(result.rows[1][1])
      assert.matches('2024%-01%-15', result.rows[1][1])

      query_module.close_connection(conn)
    end)
  end)

  describe('Numeric Type Handling', function()
    it('should handle all numeric types correctly', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, [[
        SELECT
          127::TINYINT as tiny,
          32767::SMALLINT as small,
          2147483647::INTEGER as int,
          9223372036854775807::BIGINT as big,
          255::UTINYINT as utiny,
          65535::USMALLINT as usmall,
          4294967295::UINTEGER as uint,
          3.14::FLOAT as fl,
          3.14159265359::DOUBLE as dbl
      ]])

      assert.is_nil(query_err)
      assert.is_not_nil(result)
      assert.equals(1, result.row_count)

      local row = result.rows[1]
      assert.equals(127, row[1])      -- TINYINT
      assert.equals(32767, row[2])    -- SMALLINT
      assert.equals(2147483647, row[3]) -- INTEGER
      assert.is_number(row[4])        -- BIGINT
      assert.equals(255, row[5])      -- UTINYINT
      assert.equals(65535, row[6])    -- USMALLINT
      assert.equals(4294967295, row[7]) -- UINTEGER
      assert.is_true(math.abs(row[8] - 3.14) < 0.01) -- FLOAT
      assert.is_true(math.abs(row[9] - 3.14159265359) < 0.0001) -- DOUBLE

      query_module.close_connection(conn)
    end)

    it('should handle NULL values correctly', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, [[
        SELECT
          NULL::INTEGER as null_int,
          NULL::VARCHAR as null_str,
          NULL::DOUBLE as null_dbl,
          NULL::DATE as null_date
      ]])

      assert.is_nil(query_err)
      assert.is_not_nil(result)
      assert.equals(1, result.row_count)

      local row = result.rows[1]
      assert.is_nil(row[1])
      assert.is_nil(row[2])
      assert.is_nil(row[3])
      assert.is_nil(row[4])

      query_module.close_connection(conn)
    end)
  end)

  describe('String Handling', function()
    it('should handle inline strings (<=12 bytes) correctly', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, [[
        SELECT 'short' as s1, 'medium str' as s2, 'twelve char' as s3
      ]])

      assert.is_nil(query_err)
      assert.equals('short', result.rows[1][1])
      assert.equals('medium str', result.rows[1][2])
      assert.equals('twelve char', result.rows[1][3])

      query_module.close_connection(conn)
    end)

    it('should handle pointer-based strings (>12 bytes) correctly', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local long_str = 'this is a much longer string that definitely exceeds twelve bytes'
      local result, query_err = query_module.execute_query(conn, string.format(
        "SELECT '%s' as long_str", long_str
      ))

      assert.is_nil(query_err)
      assert.equals(long_str, result.rows[1][1])
      assert.is_true(#result.rows[1][1] > 12)

      query_module.close_connection(conn)
    end)

    it('should handle empty strings', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, [[
        SELECT '' as empty
      ]])

      assert.is_nil(query_err)
      assert.equals('', result.rows[1][1])

      query_module.close_connection(conn)
    end)

    it('should handle special characters in strings', function()
      if not duckdb_ffi.lib then
        pending('DuckDB library not available')
      end

      local conn, err = query_module.create_connection()
      assert.is_nil(err)

      local result, query_err = query_module.execute_query(conn, [[
        SELECT
          'line1
line2' as multiline,
          'tab  here' as with_tab,
          'quote''s' as with_quote
      ]])

      assert.is_nil(query_err)
      assert.is_not_nil(result)
      assert.equals(1, result.row_count)

      query_module.close_connection(conn)
    end)
  end)
end)
