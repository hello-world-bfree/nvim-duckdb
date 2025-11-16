-- Luacheck configuration for nvim-duckdb

-- Standard globals
std = "luajit"

-- Neovim globals
globals = {
  "vim",
}

-- Test globals (plenary.nvim)
files["tests/**/*_spec.lua"] = {
  globals = {
    "describe",
    "it",
    "before_each",
    "after_each",
    "setup",
    "teardown",
    "pending",
    "assert",
  }
}

-- Ignore certain warnings
ignore = {
  "212", -- Unused argument
  "631", -- Line is too long (we handle this with max_line_length)
}

-- Maximum line length
max_line_length = 120

-- Allow unused loop variables
unused_args = false

-- Exclude certain directories
exclude_files = {
  ".luarocks/",
  "lua_modules/",
}
