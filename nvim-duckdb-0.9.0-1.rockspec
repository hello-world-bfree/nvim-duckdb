rockspec_format = "3.0"
package = "nvim-duckdb"
version = "0.9.0-1"

source = {
   url = "git://github.com/hello-world-bfree/nvim-duckdb",
   tag = "v0.9.0"
}

description = {
   summary = "Neovim plugin for DuckDB integration via LuaJIT FFI",
   detailed = [[
      A Neovim plugin that integrates DuckDB via LuaJIT FFI to enable SQL queries
      on CSV and JSON files directly from buffers. Features include:

      - Direct buffer queries without saving files
      - Multiple format support (CSV, JSON, JSONL)
      - Multi-buffer joins with buffer('name') syntax
      - Interactive floating window results
      - Export capabilities (CSV, JSON, formatted tables)
      - Schema inspection with :DuckDBSchema
      - Smart validation leveraging DuckDB's parser
      - Full SQL support with DuckDB's analytical capabilities
   ]],
   homepage = "https://github.com/hello-world-bfree/nvim-duckdb",
   license = "MIT",
   issues_url = "https://github.com/hello-world-bfree/nvim-duckdb/issues",
   maintainer = "hello-world-bfree"
}

dependencies = {
   "lua >= 5.1"
}

external_dependencies = {
   DUCKDB = {
      header = "duckdb.h",
      library = "duckdb"
   }
}

build = {
   type = "builtin",
   modules = {
      ["duckdb"] = "lua/duckdb/init.lua",
      ["duckdb.buffer"] = "lua/duckdb/buffer.lua",
      ["duckdb.ffi"] = "lua/duckdb/ffi.lua",
      ["duckdb.health"] = "lua/duckdb/health.lua",
      ["duckdb.query"] = "lua/duckdb/query.lua",
      ["duckdb.ui"] = "lua/duckdb/ui.lua",
      ["duckdb.validate"] = "lua/duckdb/validate.lua"
   },
   copy_directories = {
      "plugin"
   }
}
