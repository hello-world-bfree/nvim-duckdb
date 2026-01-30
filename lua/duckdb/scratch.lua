---@class DuckDBScratch
local M = {}

local scratch_bufnr = nil

---Get the scratch file path
---@param config table? Plugin config with scratch_path option
---@return string
function M.get_scratch_path(config)
  if config and config.scratch_path then
    return config.scratch_path
  end
  return vim.fn.stdpath("cache") .. "/duckdb_scratch.sql"
end

---Extract the current SQL statement at cursor
---@param bufnr number
---@return string? query
---@return number? start_line
---@return number? end_line
function M.get_statement_at_cursor(bufnr)
  local cursor = vim.api.nvim_win_get_cursor(0)
  local line_num = cursor[1]
  local lines = vim.api.nvim_buf_get_lines(bufnr, 0, -1, false)

  if #lines == 0 then
    return nil
  end

  local start_line = line_num
  local end_line = line_num

  while start_line > 1 do
    local prev_line = lines[start_line - 1]
    if prev_line:match("^%s*$") then
      break
    end
    if prev_line:match(";%s*$") then
      break
    end
    start_line = start_line - 1
  end

  while end_line < #lines do
    local curr_line = lines[end_line]
    if curr_line:match(";%s*$") then
      break
    end
    local next_line = lines[end_line + 1]
    if next_line and next_line:match("^%s*$") then
      break
    end
    end_line = end_line + 1
  end

  local statement_lines = {}
  for i = start_line, end_line do
    table.insert(statement_lines, lines[i])
  end

  local query = table.concat(statement_lines, "\n"):gsub(";%s*$", "")
  if query:match("^%s*$") then
    return nil
  end

  return query, start_line, end_line
end

---Execute statement at cursor
---@param bufnr number
function M.execute_at_cursor(bufnr)
  local query = M.get_statement_at_cursor(bufnr)
  if not query then
    vim.notify("[DuckDB] No SQL statement at cursor", vim.log.levels.WARN)
    return
  end

  local duckdb = require("duckdb")
  duckdb.query(query)
end

---Open or focus the scratch buffer
---@param config table? Plugin config
---@return number bufnr
function M.open(config)
  local scratch_path = M.get_scratch_path(config)

  if scratch_bufnr and vim.api.nvim_buf_is_valid(scratch_bufnr) then
    local wins = vim.fn.win_findbuf(scratch_bufnr)
    if #wins > 0 then
      vim.api.nvim_set_current_win(wins[1])
    else
      vim.cmd("split")
      vim.api.nvim_win_set_buf(0, scratch_bufnr)
    end
    return scratch_bufnr
  end

  local dir = vim.fn.fnamemodify(scratch_path, ":h")
  if vim.fn.isdirectory(dir) == 0 then
    vim.fn.mkdir(dir, "p")
  end

  if vim.fn.filereadable(scratch_path) == 0 then
    local file = io.open(scratch_path, "w")
    if file then
      file:write("-- DuckDB Scratch Buffer\n")
      file:write("-- Press <CR> to execute the statement under cursor\n")
      file:write("-- Statements are separated by semicolons or blank lines\n\n")
      file:write("SELECT * FROM buffer LIMIT 10;\n")
      file:close()
    end
  end

  vim.cmd("split " .. vim.fn.fnameescape(scratch_path))
  scratch_bufnr = vim.api.nvim_get_current_buf()

  vim.api.nvim_set_option_value("filetype", "sql", { buf = scratch_bufnr })

  local opts = { buffer = scratch_bufnr, silent = true }

  vim.keymap.set("n", "<CR>", function()
    M.execute_at_cursor(scratch_bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Execute statement" }))

  vim.keymap.set("n", "<C-CR>", function()
    M.execute_at_cursor(scratch_bufnr)
  end, vim.tbl_extend("force", opts, { desc = "DuckDB: Execute statement" }))

  vim.api.nvim_create_autocmd("BufWritePost", {
    buffer = scratch_bufnr,
    callback = function()
      vim.notify("[DuckDB] Scratch saved", vim.log.levels.INFO)
    end,
  })

  return scratch_bufnr
end

---Close the scratch buffer
function M.close()
  if scratch_bufnr and vim.api.nvim_buf_is_valid(scratch_bufnr) then
    local wins = vim.fn.win_findbuf(scratch_bufnr)
    for _, win in ipairs(wins) do
      vim.api.nvim_win_close(win, false)
    end
  end
end

return M
