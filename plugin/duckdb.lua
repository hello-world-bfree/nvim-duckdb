-- DuckDB Neovim Plugin
-- Provides SQL query capabilities on CSV and JSON buffers via DuckDB

-- Prevent loading twice
if vim.g.loaded_duckdb then
  return
end
vim.g.loaded_duckdb = 1

-- Check for LuaJIT
if not jit then
  vim.notify(
    '[DuckDB] This plugin requires LuaJIT (Neovim includes this by default)',
    vim.log.levels.ERROR
  )
  return
end

local duckdb = require('duckdb')

-- Create :DuckDB command
vim.api.nvim_create_user_command('DuckDB', function(args)
  duckdb.command_handler(args)
end, {
  nargs = '*',
  range = true,
  desc = 'Execute DuckDB SQL query on buffer',
  complete = function(arg_lead, cmd_line, cursor_pos)
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
vim.api.nvim_create_user_command('DuckDBQuery', function(args)
  duckdb.command_handler(args)
end, {
  nargs = '*',
  range = true,
  desc = 'Execute DuckDB SQL query on buffer (alias)',
})

-- Create :DuckDBSchema command
vim.api.nvim_create_user_command('DuckDBSchema', function(args)
  local buffer_id = args.args ~= '' and args.args or nil
  duckdb.get_schema(buffer_id)
end, {
  nargs = '?',
  desc = 'Show schema of buffer',
})

-- Create :DuckDBBuffers command
vim.api.nvim_create_user_command('DuckDBBuffers', function()
  local buffers = duckdb.list_queryable_buffers()

  if #buffers == 0 then
    vim.notify('[DuckDB] No queryable buffers found', vim.log.levels.INFO)
    return
  end

  local lines = { 'Queryable Buffers:', '' }
  for _, buf in ipairs(buffers) do
    local name = buf.name ~= '' and buf.name or string.format('[Buffer %d]', buf.bufnr)
    table.insert(lines, string.format('  %d: %s (%s)', buf.bufnr, name, buf.filetype))
  end

  vim.notify(table.concat(lines, '\n'), vim.log.levels.INFO)
end, {
  desc = 'List all queryable buffers',
})

-- Create :DuckDBValidate command
vim.api.nvim_create_user_command('DuckDBValidate', function(args)
  local buffer_id = args.args ~= '' and args.args or nil
  duckdb.validate(buffer_id)
end, {
  nargs = '?',
  desc = 'Validate CSV/JSON buffer using DuckDB parser',
})

-- Create :DuckDBClearValidation command
vim.api.nvim_create_user_command('DuckDBClearValidation', function(args)
  local buffer_id = args.args ~= '' and args.args or nil
  duckdb.clear_validation(buffer_id)
end, {
  nargs = '?',
  desc = 'Clear validation diagnostics for buffer',
})

-- Setup default keymaps (users can override in their config)
local function setup_default_keymaps()
  -- Only set if not already mapped
  if vim.fn.mapcheck('<leader>dq', 'n') == '' then
    vim.keymap.set('n', '<leader>dq', function()
      require('duckdb').query_prompt()
    end, { desc = 'DuckDB: Query prompt' })
  end

  if vim.fn.mapcheck('<leader>ds', 'n') == '' then
    vim.keymap.set('n', '<leader>ds', function()
      require('duckdb').get_schema()
    end, { desc = 'DuckDB: Show schema' })
  end

  if vim.fn.mapcheck('<leader>dv', 'n') == '' then
    vim.keymap.set('n', '<leader>dv', function()
      require('duckdb').validate_current_buffer()
    end, { desc = 'DuckDB: Validate buffer' })
  end

  if vim.fn.mapcheck('<leader>dq', 'v') == '' then
    vim.keymap.set('v', '<leader>dq', function()
      require('duckdb').query_visual()
    end, { desc = 'DuckDB: Execute visual selection' })
  end
end

-- Setup keymaps after a short delay to allow user config to load
vim.defer_fn(function()
  -- Only setup default keymaps if user hasn't disabled them
  if vim.g.duckdb_no_default_keymaps ~= 1 then
    setup_default_keymaps()
  end
end, 100)

-- Health check is automatically discovered by Neovim
-- Run :checkhealth duckdb to use it
