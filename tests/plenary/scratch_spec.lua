describe("duckdb.scratch", function()
  local scratch

  before_each(function()
    scratch = require("duckdb.scratch")
  end)

  describe("get_scratch_path", function()
    it("should return default path when no config", function()
      local path = scratch.get_scratch_path()
      assert.truthy(path:match("duckdb_scratch%.sql$"))
    end)

    it("should use custom path from config", function()
      local path = scratch.get_scratch_path({ scratch_path = "/tmp/custom.sql" })
      assert.equals("/tmp/custom.sql", path)
    end)
  end)

  describe("get_statement_at_cursor", function()
    it("should extract single line statement", function()
      local buf = vim.api.nvim_create_buf(false, true)
      vim.api.nvim_buf_set_lines(buf, 0, -1, false, {
        "SELECT * FROM test;",
      })
      vim.api.nvim_set_current_buf(buf)
      vim.api.nvim_win_set_cursor(0, { 1, 0 })

      local query = scratch.get_statement_at_cursor(buf)
      assert.equals("SELECT * FROM test", query)

      vim.api.nvim_buf_delete(buf, { force = true })
    end)

    it("should extract multi-line statement", function()
      local buf = vim.api.nvim_create_buf(false, true)
      vim.api.nvim_buf_set_lines(buf, 0, -1, false, {
        "SELECT *",
        "FROM test",
        "WHERE id = 1;",
      })
      vim.api.nvim_set_current_buf(buf)
      vim.api.nvim_win_set_cursor(0, { 2, 0 })

      local query = scratch.get_statement_at_cursor(buf)
      assert.truthy(query:match("SELECT"))
      assert.truthy(query:match("FROM"))
      assert.truthy(query:match("WHERE"))

      vim.api.nvim_buf_delete(buf, { force = true })
    end)

    it("should handle statements separated by blank lines", function()
      local buf = vim.api.nvim_create_buf(false, true)
      vim.api.nvim_buf_set_lines(buf, 0, -1, false, {
        "SELECT 1",
        "",
        "SELECT 2",
      })
      vim.api.nvim_set_current_buf(buf)
      vim.api.nvim_win_set_cursor(0, { 1, 0 })

      local query = scratch.get_statement_at_cursor(buf)
      assert.equals("SELECT 1", query)

      vim.api.nvim_win_set_cursor(0, { 3, 0 })
      query = scratch.get_statement_at_cursor(buf)
      assert.equals("SELECT 2", query)

      vim.api.nvim_buf_delete(buf, { force = true })
    end)

    it("should return nil for empty buffer", function()
      local buf = vim.api.nvim_create_buf(false, true)
      vim.api.nvim_buf_set_lines(buf, 0, -1, false, {})
      vim.api.nvim_set_current_buf(buf)

      local query = scratch.get_statement_at_cursor(buf)
      assert.is_nil(query)

      vim.api.nvim_buf_delete(buf, { force = true })
    end)

    it("should return nil for whitespace only", function()
      local buf = vim.api.nvim_create_buf(false, true)
      vim.api.nvim_buf_set_lines(buf, 0, -1, false, {
        "   ",
        "",
        "  ",
      })
      vim.api.nvim_set_current_buf(buf)
      vim.api.nvim_win_set_cursor(0, { 1, 0 })

      local query = scratch.get_statement_at_cursor(buf)
      assert.is_nil(query)

      vim.api.nvim_buf_delete(buf, { force = true })
    end)
  end)
end)
