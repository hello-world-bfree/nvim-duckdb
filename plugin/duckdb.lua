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

-- Create :DuckDB command
vim.api.nvim_create_user_command("DuckDB", function(args)
	duckdb.command_handler(args)
end, {
	nargs = "*",
	range = true,
	desc = "Execute DuckDB SQL query on buffer",
	complete = function(arg_lead)
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

	-- Quick preview keymaps
	set_keymap("n", "<leader>dp", function()
		require("duckdb").query("SELECT * FROM buffer LIMIT 100")
	end, "DuckDB: Preview (LIMIT 100)")

	set_keymap("n", "<leader>d1", function()
		require("duckdb").query("SELECT * FROM buffer LIMIT 10")
	end, "DuckDB: Preview (LIMIT 10)")

	set_keymap("n", "<leader>d5", function()
		require("duckdb").query("SELECT * FROM buffer LIMIT 50")
	end, "DuckDB: Preview (LIMIT 50)")

	set_keymap("n", "<leader>da", function()
		require("duckdb").query("SELECT * FROM buffer")
	end, "DuckDB: Select all")

	set_keymap("n", "<leader>dn", function()
		require("duckdb").query("SELECT COUNT(*) as row_count FROM buffer")
	end, "DuckDB: Count rows")

	-- Visual mode
	set_keymap("v", "<leader>dq", function()
		require("duckdb").query_visual()
	end, "DuckDB: Execute visual selection")
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
