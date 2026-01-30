describe("duckdb.actions", function()
  local actions

  before_each(function()
    actions = require("duckdb.actions")
  end)

  local mock_result = {
    columns = { "id", "name", "amount" },
    rows = {
      { 1, "Alice", 100.50 },
      { 2, "Bob", 200.75 },
      { 3, "Charlie", nil },
    },
    row_count = 3,
    column_count = 3,
  }

  describe("format_json_array", function()
    it("should format result as JSON array", function()
      local json = actions.format_json_array(mock_result)
      local decoded = vim.json.decode(json)

      assert.equals(3, #decoded)
      assert.equals(1, decoded[1].id)
      assert.equals("Alice", decoded[1].name)
      assert.equals(100.50, decoded[1].amount)
    end)

    it("should handle NULL values", function()
      local json = actions.format_json_array(mock_result)
      local decoded = vim.json.decode(json)

      assert.is_nil(decoded[3].amount)
    end)
  end)

  describe("format_json_object", function()
    it("should format result as object keyed by first column", function()
      local json = actions.format_json_object(mock_result)
      local decoded = vim.json.decode(json)

      assert.truthy(decoded["1"])
      assert.equals("Alice", decoded["1"].name)
      assert.equals(100.50, decoded["1"].amount)
    end)
  end)

  describe("format_json_single", function()
    it("should format first row only", function()
      local json = actions.format_json_single(mock_result)
      local decoded = vim.json.decode(json)

      assert.equals(1, decoded.id)
      assert.equals("Alice", decoded.name)
    end)

    it("should return empty object for empty result", function()
      local empty_result = {
        columns = { "id" },
        rows = {},
        row_count = 0,
        column_count = 1,
      }
      local json = actions.format_json_single(empty_result)
      assert.equals("{}", json)
    end)
  end)

  describe("format_csv", function()
    it("should format result as CSV", function()
      local csv = actions.format_csv(mock_result)
      local lines = vim.split(csv, "\n")

      assert.equals("id,name,amount", lines[1])
      assert.truthy(lines[2]:match("1,Alice"))
    end)
  end)

  describe("_apply_sort", function()
    it("should add ORDER BY clause to query without ORDER BY", function()
      pending("Integration test - requires query execution")
    end)
  end)
end)
