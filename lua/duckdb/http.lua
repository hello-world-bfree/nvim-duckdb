---@class DuckDBHttp
local M = {}

---@class HttpOptions
---@field headers table<string, string>? Custom headers
---@field auth_env string? Environment variable name for auth token
---@field method string? HTTP method (default: POST)
---@field content_type string? Content type (default: application/json)

---POST JSON data to a URL
---@param url string
---@param data string JSON data
---@param opts HttpOptions?
---@param callback function? Callback(success, response)
function M.post_json(url, data, opts, callback)
  opts = opts or {}

  local headers = opts.headers or {}
  headers["Content-Type"] = opts.content_type or "application/json"

  if opts.auth_env then
    local token = os.getenv(opts.auth_env)
    if token then
      headers["Authorization"] = "Bearer " .. token
    end
  end

  local curl_args = { "curl", "-s", "-X", opts.method or "POST" }

  for key, value in pairs(headers) do
    table.insert(curl_args, "-H")
    table.insert(curl_args, key .. ": " .. value)
  end

  table.insert(curl_args, "-d")
  table.insert(curl_args, data)
  table.insert(curl_args, url)

  if vim.system then
    vim.system(curl_args, { text = true }, function(result)
      vim.schedule(function()
        local success = result.code == 0
        if callback then
          callback(success, result.stdout or result.stderr)
        else
          if success then
            vim.notify("[DuckDB] POST successful", vim.log.levels.INFO)
            if result.stdout and result.stdout ~= "" then
              local preview = result.stdout:sub(1, 200)
              if #result.stdout > 200 then
                preview = preview .. "..."
              end
              vim.notify(preview, vim.log.levels.INFO)
            end
          else
            vim.notify("[DuckDB] POST failed: " .. (result.stderr or "unknown error"), vim.log.levels.ERROR)
          end
        end
      end)
    end)
  else
    local cmd = table.concat(vim.tbl_map(function(arg)
      return vim.fn.shellescape(arg)
    end, curl_args), " ")

    vim.fn.jobstart(cmd, {
      on_stdout = function(_, stdout_data)
        if callback then
          callback(true, table.concat(stdout_data, "\n"))
        else
          vim.schedule(function()
            vim.notify("[DuckDB] POST successful", vim.log.levels.INFO)
          end)
        end
      end,
      on_stderr = function(_, stderr_data)
        local err = table.concat(stderr_data, "\n")
        if err ~= "" then
          vim.schedule(function()
            vim.notify("[DuckDB] POST error: " .. err, vim.log.levels.ERROR)
          end)
        end
      end,
      on_exit = function(_, code)
        if code ~= 0 and not callback then
          vim.schedule(function()
            vim.notify("[DuckDB] POST failed with code " .. code, vim.log.levels.ERROR)
          end)
        end
      end,
    })
  end
end

---POST query results to a URL
---@param url string
---@param result QueryResult
---@param opts HttpOptions?
function M.post_result(url, result, opts)
  local actions = require("duckdb.actions")
  local json = actions.format_json_array(result)
  M.post_json(url, json, opts)
end

---Interactive POST command
---@param url string
---@param bufnr number? Result buffer number
function M.interactive_post(url, bufnr)
  bufnr = bufnr or vim.api.nvim_get_current_buf()
  local metadata = vim.b[bufnr].duckdb_metadata

  if not metadata or not metadata.result then
    vim.notify("[DuckDB] No query result in current buffer", vim.log.levels.ERROR)
    return
  end

  vim.ui.select({ "Array", "Object (keyed by first column)", "Single (first row)" }, {
    prompt = "JSON Format:",
  }, function(format_choice)
    if not format_choice then
      return
    end

    local actions = require("duckdb.actions")
    local json
    if format_choice:find("Array") then
      json = actions.format_json_array(metadata.result)
    elseif format_choice:find("Object") then
      json = actions.format_json_object(metadata.result)
    else
      json = actions.format_json_single(metadata.result)
    end

    vim.notify("[DuckDB] Posting " .. #json .. " bytes to " .. url, vim.log.levels.INFO)
    M.post_json(url, json)
  end)
end

return M
