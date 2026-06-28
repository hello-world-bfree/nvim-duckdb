-- DuckDB Neovim Plugin
-- Provides SQL query capabilities on CSV and JSON buffers via DuckDB

-- Prevent loading twice
if vim.g.loaded_duckdb then
  return
end
vim.g.loaded_duckdb = 1

-- Check for LuaJIT
if not jit then
  vim.notify("[DuckDB] This plugin requires LuaJIT (Neovim includes this by default)", vim.log.levels.ERROR)
  return
end

local duckdb = require("duckdb")

-- Create :DuckDB command with buffer name completion
vim.api.nvim_create_user_command("DuckDB", function(args)
  duckdb.command_handler(args)
end, {
  nargs = "*",
  range = true,
  desc = "Execute DuckDB SQL query on buffer",
  complete = function(arg_lead, cmd_line)
    -- Check if completing buffer name inside buffer('...')
    local buffer_prefix = cmd_line:match("buffer%s*%(['\"]([^'\"]*)")
    if buffer_prefix then
      local buffer_names = duckdb.get_buffer_names()
      return vim.tbl_filter(function(name)
        return name:lower():find(buffer_prefix:lower(), 1, true) == 1
      end, buffer_names)
    end

    local completions = duckdb.get_sql_completions()
    local matches = {}

    for _, keyword in ipairs(completions) do
      if keyword:lower():find(arg_lead:lower(), 1, true) == 1 then
        table.insert(matches, keyword)
      end
    end

    return matches
  end,
})

-- Create :DuckDBQuery command (alias)
vim.api.nvim_create_user_command("DuckDBQuery", function(args)
  duckdb.command_handler(args)
end, {
  nargs = "*",
  range = true,
  desc = "Execute DuckDB SQL query on buffer (alias)",
})

-- Create :DuckDBCancel command (interrupt a running async query)
vim.api.nvim_create_user_command("DuckDBCancel", function()
  if not duckdb.cancel() then
    vim.notify("[DuckDB] No query is currently running", vim.log.levels.INFO)
  end
end, {
  desc = "Cancel the currently running DuckDB query",
})

-- Create :DuckDBParam command (parameterized query with inline values)
-- Usage: :DuckDBParam SELECT * FROM buffer WHERE id > $1 -- 42
-- Everything before " -- " is the SQL; whitespace-separated tokens after are
-- bound positionally to $1..$N (use 'NULL' for a SQL NULL).
vim.api.nvim_create_user_command("DuckDBParam", function(args)
  local sql, rest = args.args:match("^(.-)%s+%-%-%s+(.*)$")
  if not sql then
    vim.notify('[DuckDB] Usage: :DuckDBParam <sql> -- <val1> <val2> ...', vim.log.levels.ERROR)
    return
  end

  -- Split the value list on whitespace. 'NULL' (uppercase) binds SQL NULL via
  -- the NULL sentinel, which keeps its array position (a Lua nil would shift it).
  local NULL = require("duckdb.query").NULL
  local values = {}
  for i, token in ipairs(vim.split(rest, "%s+", { trimempty = true })) do
    values[i] = (token == "NULL") and NULL or token
  end

  duckdb.query_params_async(sql, values)
end, {
  nargs = "+",
  desc = "Run a parameterized query: <sql> -- <values...>",
})

-- Create :DuckDBImport command (bulk-load delimited lines via the appender)
-- Usage: :DuckDBImport [table_name]   (uses visual range if given, else buffer)
-- First line is the header; remaining lines are data rows (comma-delimited).
vim.api.nvim_create_user_command("DuckDBImport", function(args)
  local lines
  if args.range > 0 then
    lines = vim.fn.getline(args.line1, args.line2)
  else
    lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  end
  local opts = {}
  if args.args ~= "" then
    opts.table_name = args.args
  end
  duckdb.import_lines_async(lines, opts)
end, {
  nargs = "?",
  range = true,
  desc = "Bulk-import delimited lines (header + rows) into a DuckDB table",
})

-- Create :DuckDBScript command (run the current buffer as a multi-statement script)
vim.api.nvim_create_user_command("DuckDBScript", function(args)
  local bufnr = vim.api.nvim_get_current_buf()
  local script
  if args.range > 0 then
    script = table.concat(vim.fn.getline(args.line1, args.line2), "\n")
  else
    script = table.concat(vim.api.nvim_buf_get_lines(bufnr, 0, -1, false), "\n")
  end
  if script:match("^%s*$") then
    vim.notify("[DuckDB] Nothing to run", vim.log.levels.WARN)
    return
  end
  duckdb.query_script_async(script)
end, {
  range = true,
  desc = "Run the current buffer (or range) as a multi-statement SQL script",
})

-- Create :DuckDBSchema command
vim.api.nvim_create_user_command("DuckDBSchema", function(args)
  local buffer_id = args.args ~= "" and args.args or nil
  duckdb.get_schema(buffer_id)
end, {
  nargs = "?",
  desc = "Show schema of buffer",
})

-- Create :DuckDBBuffers command
vim.api.nvim_create_user_command("DuckDBBuffers", function()
  local buffers = duckdb.list_queryable_buffers()

  if #buffers == 0 then
    vim.notify("[DuckDB] No queryable buffers found", vim.log.levels.INFO)
    return
  end

  local lines = { "Queryable Buffers:", "" }
  for _, buf in ipairs(buffers) do
    local name = buf.name ~= "" and buf.name or string.format("[Buffer %d]", buf.bufnr)
    table.insert(lines, string.format("  %d: %s (%s)", buf.bufnr, name, buf.filetype))
  end

  vim.notify(table.concat(lines, "\n"), vim.log.levels.INFO)
end, {
  desc = "List all queryable buffers",
})

-- Create :DuckDBValidate command
vim.api.nvim_create_user_command("DuckDBValidate", function(args)
  local buffer_id = args.args ~= "" and args.args or nil
  duckdb.validate(buffer_id)
end, {
  nargs = "?",
  desc = "Validate CSV/JSON buffer using DuckDB parser",
})

-- Create :DuckDBConvert command
vim.api.nvim_create_user_command("DuckDBConvert", function(args)
  local parts = vim.split(args.args, " ", { trimempty = true })
  local format = parts[1]
  local output_path = parts[2]

  if not format then
    vim.notify("[DuckDB] Usage: :DuckDBConvert <format> [output_path]", vim.log.levels.ERROR)
    vim.notify("[DuckDB] Formats: csv, json, jsonl, parquet", vim.log.levels.INFO)
    return
  end

  duckdb.convert(format, output_path)
end, {
  nargs = "+",
  desc = "Convert buffer to another format (csv, json, jsonl, parquet)",
  complete = function(arg_lead, cmd_line)
    local parts = vim.split(cmd_line, " ", { trimempty = true })
    if #parts <= 2 then
      -- Complete format
      local formats = { "csv", "json", "jsonl", "parquet" }
      return vim.tbl_filter(function(f)
        return f:find(arg_lead, 1, true) == 1
      end, formats)
    else
      -- Complete file path
      return vim.fn.getcompletion(arg_lead, "file")
    end
  end,
})

-- Create :DuckDBScratch command
vim.api.nvim_create_user_command("DuckDBScratch", function()
  duckdb.scratch()
end, {
  desc = "Open persistent SQL scratch buffer",
})

-- Create :DuckDBHistory command
vim.api.nvim_create_user_command("DuckDBHistory", function()
  duckdb.history()
end, {
  desc = "Browse and rerun query history",
})

-- Create :DuckDBSummary command
vim.api.nvim_create_user_command("DuckDBSummary", function(args)
  local buffer_id = args.args ~= "" and args.args or nil
  duckdb.summary(buffer_id)
end, {
  nargs = "?",
  desc = "Show SUMMARIZE statistics for buffer",
})

-- Create :DuckDBHover command
vim.api.nvim_create_user_command("DuckDBHover", function()
  duckdb.hover()
end, {
  desc = "Show column stats under cursor",
})

-- Create :DuckDBPost command
vim.api.nvim_create_user_command("DuckDBPost", function(args)
  if args.args == "" then
    vim.notify("[DuckDB] Usage: :DuckDBPost <url>", vim.log.levels.ERROR)
    return
  end
  duckdb.post(args.args)
end, {
  nargs = 1,
  desc = "POST query results to URL",
})

-- Create :DuckDBTransform command
vim.api.nvim_create_user_command("DuckDBTransform", function(args)
  if args.args == "" then
    vim.ui.input({
      prompt = "Transform query: ",
      default = "SELECT * FROM buffer",
    }, function(query)
      if query and query ~= "" then
        duckdb.transform(query)
      end
    end)
  else
    duckdb.transform(args.args)
  end
end, {
  nargs = "*",
  desc = "Transform data with diff preview",
})

-- Setup default keymaps (users can override in their config)
local function setup_default_keymaps()
  -- Helper to set keymap only if not already mapped
  local function set_keymap(mode, lhs, rhs, desc)
    if vim.fn.mapcheck(lhs, mode) == "" then
      vim.keymap.set(mode, lhs, rhs, { desc = desc })
    end
  end

  -- Core keymaps
  set_keymap("n", "<leader>dq", function()
    require("duckdb").query_prompt()
  end, "DuckDB: Query prompt")

  set_keymap("n", "<leader>ds", function()
    require("duckdb").get_schema()
  end, "DuckDB: Show schema")

  set_keymap("n", "<leader>dv", function()
    require("duckdb").validate_current_buffer()
  end, "DuckDB: Validate buffer")

  set_keymap("n", "<leader>db", function()
    vim.cmd("DuckDBBuffers")
  end, "DuckDB: List buffers")

  set_keymap("n", "<leader>dc", function()
    vim.cmd("DuckDBCancel")
  end, "DuckDB: Cancel running query")

  -- Quick preview keymaps (async: non-blocking, cancellable via <leader>dc)
  set_keymap("n", "<leader>dp", function()
    require("duckdb").query_async("SELECT * FROM buffer LIMIT 100")
  end, "DuckDB: Preview (LIMIT 100)")

  set_keymap("n", "<leader>d1", function()
    require("duckdb").query_async("SELECT * FROM buffer LIMIT 10")
  end, "DuckDB: Preview (LIMIT 10)")

  set_keymap("n", "<leader>d5", function()
    require("duckdb").query_async("SELECT * FROM buffer LIMIT 50")
  end, "DuckDB: Preview (LIMIT 50)")

  set_keymap("n", "<leader>da", function()
    require("duckdb").query_async("SELECT * FROM buffer")
  end, "DuckDB: Select all")

  set_keymap("n", "<leader>dn", function()
    require("duckdb").query_async("SELECT COUNT(*) as row_count FROM buffer")
  end, "DuckDB: Count rows")

  -- New keymaps for Phase 1-3 features
  set_keymap("n", "<leader>dh", function()
    require("duckdb").history()
  end, "DuckDB: Query history")

  set_keymap("n", "<leader>dS", function()
    require("duckdb").scratch()
  end, "DuckDB: Open scratch buffer")

  set_keymap("n", "<leader>du", function()
    require("duckdb").summary()
  end, "DuckDB: Show summary stats")

  set_keymap("n", "<leader>dt", function()
    vim.ui.input({
      prompt = "Transform query: ",
      default = "SELECT * FROM buffer",
    }, function(query)
      if query and query ~= "" then
        require("duckdb").transform(query)
      end
    end)
  end, "DuckDB: Transform with diff")

  -- Visual mode
  set_keymap("v", "<leader>dq", function()
    require("duckdb").query_visual()
  end, "DuckDB: Execute visual selection")
end

-- Setup K mapping for hover stats on CSV/JSON files
local function setup_hover_keymap()
  vim.api.nvim_create_autocmd("FileType", {
    pattern = { "csv", "json", "jsonl" },
    callback = function(event)
      vim.keymap.set("n", "K", function()
        require("duckdb").hover()
      end, { buffer = event.buf, desc = "DuckDB: Show column stats" })
    end,
  })
end

-- Setup keymaps after a short delay to allow user config to load
vim.defer_fn(function()
  -- Only setup default keymaps if user hasn't disabled them
  if vim.g.duckdb_no_default_keymaps ~= 1 then
    setup_default_keymaps()
    setup_hover_keymap()
  end
end, 100)

-- Health check is automatically discovered by Neovim
-- Run :checkhealth duckdb to use it
