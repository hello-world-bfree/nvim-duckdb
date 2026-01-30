---@class DuckDBTelescope
local M = {}

local has_telescope, telescope = pcall(require, "telescope")
if not has_telescope then
  return setmetatable({}, {
    __index = function()
      return function()
        vim.notify("[DuckDB] Telescope not found", vim.log.levels.WARN)
      end
    end,
  })
end

local pickers = require("telescope.pickers")
local finders = require("telescope.finders")
local conf = require("telescope.config").values
local actions = require("telescope.actions")
local action_state = require("telescope.actions.state")
local previewers = require("telescope.previewers")

---Query history picker
---@param opts table?
function M.history(opts)
  opts = opts or {}

  local history = require("duckdb.history")
  local entries = history.get({ limit = 100 })

  if #entries == 0 then
    vim.notify("[DuckDB] No query history", vim.log.levels.INFO)
    return
  end

  pickers
    .new(opts, {
      prompt_title = "DuckDB Query History",
      finder = finders.new_table({
        results = entries,
        entry_maker = function(entry)
          local time_str = os.date("%Y-%m-%d %H:%M", entry.timestamp)
          local query_preview = entry.query:gsub("\n", " "):sub(1, 80)
          return {
            value = entry,
            display = string.format("[%s] %s", time_str, query_preview),
            ordinal = entry.query,
          }
        end,
      }),
      sorter = conf.generic_sorter(opts),
      previewer = previewers.new_buffer_previewer({
        title = "Query",
        define_preview = function(self, entry)
          local lines = vim.split(entry.value.query, "\n")
          table.insert(lines, "")
          table.insert(lines, string.format("-- Rows: %d", entry.value.row_count or 0))
          table.insert(lines, string.format("-- Time: %dms", entry.value.execution_time_ms or 0))
          if entry.value.buffer_name then
            table.insert(lines, string.format("-- Buffer: %s", entry.value.buffer_name))
          end
          vim.api.nvim_buf_set_lines(self.state.bufnr, 0, -1, false, lines)
          vim.api.nvim_set_option_value("filetype", "sql", { buf = self.state.bufnr })
        end,
      }),
      attach_mappings = function(prompt_bufnr, map)
        actions.select_default:replace(function()
          actions.close(prompt_bufnr)
          local selection = action_state.get_selected_entry()
          if selection then
            local duckdb = require("duckdb")
            duckdb.query(selection.value.query)
          end
        end)

        map("i", "<C-y>", function()
          local selection = action_state.get_selected_entry()
          if selection then
            vim.fn.setreg("+", selection.value.query)
            vim.fn.setreg('"', selection.value.query)
            vim.notify("[DuckDB] Query copied", vim.log.levels.INFO)
          end
        end)

        return true
      end,
    })
    :find()
end

---Buffer picker for DuckDB queries
---@param opts table?
function M.buffers(opts)
  opts = opts or {}

  local duckdb = require("duckdb")
  local buffers = duckdb.list_queryable_buffers()

  if #buffers == 0 then
    vim.notify("[DuckDB] No queryable buffers", vim.log.levels.INFO)
    return
  end

  pickers
    .new(opts, {
      prompt_title = "DuckDB Buffers",
      finder = finders.new_table({
        results = buffers,
        entry_maker = function(buf)
          local name = buf.name ~= "" and vim.fn.fnamemodify(buf.name, ":t") or "[No Name]"
          return {
            value = buf,
            display = string.format("%d: %s (%s)", buf.bufnr, name, buf.filetype),
            ordinal = name,
          }
        end,
      }),
      sorter = conf.generic_sorter(opts),
      attach_mappings = function(prompt_bufnr)
        actions.select_default:replace(function()
          actions.close(prompt_bufnr)
          local selection = action_state.get_selected_entry()
          if selection then
            vim.ui.input({
              prompt = "Query: ",
              default = "SELECT * FROM buffer LIMIT 10",
            }, function(query)
              if query and query ~= "" then
                local duckdb_mod = require("duckdb")
                duckdb_mod.query(query, { buffer = selection.value.bufnr })
              end
            end)
          end
        end)
        return true
      end,
    })
    :find()
end

---Result rows picker with filtering
---@param result QueryResult
---@param opts table?
function M.results(result, opts)
  opts = opts or {}

  if not result or #result.rows == 0 then
    vim.notify("[DuckDB] No results to display", vim.log.levels.INFO)
    return
  end

  local entries = {}
  for i, row in ipairs(result.rows) do
    local parts = {}
    for j, val in ipairs(row) do
      local col = result.columns[j] or string.format("col%d", j)
      table.insert(parts, string.format("%s=%s", col, tostring(val or "NULL")))
    end
    table.insert(entries, {
      idx = i,
      row = row,
      display = table.concat(parts, " | "),
    })
  end

  pickers
    .new(opts, {
      prompt_title = string.format("DuckDB Results (%d rows)", #result.rows),
      finder = finders.new_table({
        results = entries,
        entry_maker = function(entry)
          return {
            value = entry,
            display = entry.display,
            ordinal = entry.display,
          }
        end,
      }),
      sorter = conf.generic_sorter(opts),
      previewer = previewers.new_buffer_previewer({
        title = "Row Details",
        define_preview = function(self, entry)
          local lines = {}
          for i, col in ipairs(result.columns) do
            local val = entry.value.row[i]
            table.insert(lines, string.format("%s: %s", col, tostring(val or "NULL")))
          end
          vim.api.nvim_buf_set_lines(self.state.bufnr, 0, -1, false, lines)
        end,
      }),
      attach_mappings = function(prompt_bufnr, map)
        map("i", "<C-y>", function()
          local selection = action_state.get_selected_entry()
          if selection then
            local obj = {}
            for i, col in ipairs(result.columns) do
              obj[col] = selection.value.row[i]
            end
            local json = vim.json.encode(obj)
            vim.fn.setreg("+", json)
            vim.fn.setreg('"', json)
            vim.notify("[DuckDB] Row copied as JSON", vim.log.levels.INFO)
          end
        end)

        actions.select_default:replace(function()
          local selection = action_state.get_selected_entry()
          if selection then
            local obj = {}
            for i, col in ipairs(result.columns) do
              obj[col] = selection.value.row[i]
            end
            local json = vim.json.encode(obj)
            vim.fn.setreg("+", json)
            vim.fn.setreg('"', json)
            vim.notify("[DuckDB] Row copied as JSON", vim.log.levels.INFO)
          end
          actions.close(prompt_bufnr)
        end)

        return true
      end,
    })
    :find()
end

function M.setup()
  telescope.register_extension({
    exports = {
      history = M.history,
      buffers = M.buffers,
      results = M.results,
    },
  })
end

return M
