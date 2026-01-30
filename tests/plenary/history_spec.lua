describe("duckdb.history", function()
  local history

  before_each(function()
    history = require("duckdb.history")
    history.clear()
  end)

  describe("add", function()
    it("should add entries to history", function()
      history.add({
        query = "SELECT * FROM test",
        timestamp = os.time(),
        row_count = 10,
        execution_time_ms = 50,
      })

      local entries = history.get()
      assert.equals(1, #entries)
      assert.equals("SELECT * FROM test", entries[1].query)
    end)

    it("should maintain order (newest first)", function()
      history.add({
        query = "SELECT 1",
        timestamp = 1000,
        row_count = 1,
        execution_time_ms = 10,
      })
      history.add({
        query = "SELECT 2",
        timestamp = 2000,
        row_count = 1,
        execution_time_ms = 10,
      })

      local entries = history.get()
      assert.equals(2, #entries)
      assert.equals("SELECT 2", entries[1].query)
      assert.equals("SELECT 1", entries[2].query)
    end)

    it("should respect limit", function()
      for i = 1, 10 do
        history.add({
          query = "SELECT " .. i,
          timestamp = os.time() + i,
          row_count = i,
          execution_time_ms = i * 10,
        }, 5)
      end

      local entries = history.get()
      assert.equals(5, #entries)
    end)
  end)

  describe("search", function()
    it("should filter by query text", function()
      history.add({
        query = "SELECT * FROM users",
        timestamp = os.time(),
        row_count = 10,
        execution_time_ms = 50,
      })
      history.add({
        query = "SELECT * FROM orders",
        timestamp = os.time(),
        row_count = 20,
        execution_time_ms = 60,
      })

      local results = history.search("users")
      assert.equals(1, #results)
      assert.equals("SELECT * FROM users", results[1].query)
    end)

    it("should be case insensitive", function()
      history.add({
        query = "SELECT * FROM USERS",
        timestamp = os.time(),
        row_count = 10,
        execution_time_ms = 50,
      })

      local results = history.search("users")
      assert.equals(1, #results)
    end)
  end)

  describe("recent", function()
    it("should return last N entries", function()
      for i = 1, 10 do
        history.add({
          query = "SELECT " .. i,
          timestamp = os.time() + i,
          row_count = i,
          execution_time_ms = i * 10,
        })
      end

      local recent = history.recent(3)
      assert.equals(3, #recent)
    end)
  end)

  describe("clear", function()
    it("should remove all entries", function()
      history.add({
        query = "SELECT 1",
        timestamp = os.time(),
        row_count = 1,
        execution_time_ms = 10,
      })

      history.clear()
      local entries = history.get()
      assert.equals(0, #entries)
    end)
  end)
end)
