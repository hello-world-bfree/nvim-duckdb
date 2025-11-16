-- Minimal init.lua for running tests
-- This sets up the runtime path to include the plugin and test dependencies

local plenary_dir = os.getenv('PLENARY_DIR') or '/tmp/plenary.nvim'
local plugin_dir = vim.fn.fnamemodify(vim.fn.resolve(vim.fn.expand('<sfile>:p')), ':h:h')

-- Add plugin to runtimepath
vim.opt.runtimepath:append(plugin_dir)
vim.opt.runtimepath:append(plenary_dir)

-- Load plenary if available (for advanced tests)
local ok, _ = pcall(require, 'plenary')
if not ok then
  vim.notify('Plenary not found, some tests may be limited', vim.log.levels.WARN)
end

-- Set up basic vim options for testing
vim.o.swapfile = false
vim.o.backup = false
vim.o.writebackup = false

-- Enable loading plugin
vim.cmd([[runtime plugin/duckdb.lua]])
