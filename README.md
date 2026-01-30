<div align="center">
  <img src="assets/logo.png" alt="nvim-duckdb - DuckDB integration for Neovim" width="400">
</div>

# nvim-duckdb

A Neovim plugin that integrates DuckDB via LuaJIT FFI to enable SQL queries on CSV and JSON files directly from buffers.

## Features

- **Direct Buffer Queries**: Query CSV, JSON, and JSONL data without saving files
- **Multi-Buffer Joins**: Query and join data across multiple buffers
- **Query History**: Persistent history with search and re-execution
- **SQL Scratch Buffer**: Dedicated buffer for writing and executing queries
- **Result Actions**: Filter, sort, export, and yank results in multiple formats
- **Schema Statistics**: SUMMARIZE stats and column-level hover information
- **Data Validation**: Inline diagnostics for CSV/JSON parsing errors
- **Format Conversion**: Convert between CSV, JSON, JSONL, and Parquet
- **HTTP Export**: POST query results directly to APIs
- **Telescope Integration**: Optional pickers for history and buffers

---

## Tutorials

Step-by-step guides to get started with nvim-duckdb.

### Tutorial 1: Your First Query

**Goal**: Query a CSV file and view results.

1. Install DuckDB:
   ```bash
   # macOS
   brew install duckdb

   # Ubuntu/Debian
   sudo apt install libduckdb-dev
   ```

2. Install the plugin with lazy.nvim:
   ```lua
   {
     'hello-world-bfree/nvim-duckdb',
     config = function()
       require('duckdb').setup()
     end
   }
   ```

3. Create a test file `employees.csv`:
   ```csv
   name,age,department,salary
   Alice,30,Engineering,95000
   Bob,25,Marketing,65000
   Charlie,35,Engineering,105000
   ```

4. Open the file and run:
   ```vim
   :DuckDB SELECT * FROM buffer WHERE age > 28
   ```

5. A floating window shows filtered results. Press `q` to close.

### Tutorial 2: Using the Scratch Buffer

**Goal**: Write multi-line queries in a dedicated SQL buffer.

1. Open the scratch buffer:
   ```vim
   :DuckDBScratch
   ```

2. Write a query (the buffer persists across sessions):
   ```sql
   SELECT department,
          COUNT(*) as headcount,
          AVG(salary) as avg_salary
   FROM buffer
   GROUP BY department
   ORDER BY avg_salary DESC;
   ```

3. Position cursor on the query and press `<CR>` to execute.

4. The query executes against the last active data buffer.

### Tutorial 3: Working with Results

**Goal**: Filter, sort, and export query results.

1. Run a query:
   ```vim
   :DuckDB SELECT * FROM buffer
   ```

2. In the result window, use these keys:
   - `f` — Add a filter (prompts for WHERE clause)
   - `s` — Sort by column under cursor
   - `ya` — Yank as JSON array
   - `yc` — Yank as CSV
   - `e` — Export to file
   - `r` — Re-run the query

3. Try filtering: press `f`, enter `salary > 80000`, see filtered results.

### Tutorial 4: Query History

**Goal**: Browse and re-execute previous queries.

1. Run several queries to build history.

2. Open history picker:
   ```vim
   :DuckDBHistory
   ```

3. Select a query to re-execute it, or browse with arrow keys.

4. History persists across Neovim sessions.

---

## How-To Guides

Task-oriented guides for specific workflows.

### Join Data from Multiple Buffers

Open two files: `orders.csv` and `customers.csv`, then:

```vim
:DuckDB SELECT c.name, COUNT(o.id) as orders
        FROM buffer('customers.csv') c
        JOIN buffer('orders.csv') o ON c.id = o.customer_id
        GROUP BY c.name
```

Buffer references:
- `buffer` — current buffer
- `buffer('filename.csv')` — buffer by name
- `buffer(5)` — buffer by number

### Validate Data Files

Check CSV/JSON for parsing errors:

```vim
:DuckDBValidate
```

Errors appear as inline diagnostics. Navigate with `]d` and `[d`.

Clear diagnostics:
```vim
:DuckDBClearValidation
```

### View Column Statistics

On a CSV/JSON buffer, press `K` on any line to see column stats (count, nulls, min, max, unique values).

Or run SUMMARIZE for full statistics:
```vim
:DuckDBSummary
```

### Convert File Formats

Convert the current buffer to another format:

```vim
:DuckDBConvert json                    " Auto-names output
:DuckDBConvert parquet output.parquet  " Specify path
```

Supported formats: `csv`, `json`, `jsonl`, `parquet`

### POST Results to an API

After running a query, POST results to a URL:

```vim
:DuckDBPost https://api.example.com/data
```

Select JSON format (array, object keyed by first column, or single row).

### Transform Data with Diff Preview

Preview changes before applying:

```vim
:DuckDBTransform SELECT name, salary * 1.1 as new_salary FROM buffer
```

A diff view shows original vs. transformed. Choose "Apply changes" or "Cancel".

### Use Telescope Integration

If Telescope is installed:

```lua
-- In your config
require('duckdb.integrations.telescope').setup()

-- Then use
:Telescope duckdb history
:Telescope duckdb buffers
```

### Query from Visual Selection

Select SQL text in any buffer and press `<leader>dq` to execute it.

### Export Query Results

```lua
require('duckdb').query('SELECT * FROM buffer', {
  export = '/tmp/results.csv',
  format = 'csv'  -- or 'json', 'table'
})
```

### Disable Default Keymaps

```lua
vim.g.duckdb_no_default_keymaps = 1
```

---

## Reference

Complete reference for commands, keymaps, configuration, and API.

### Commands

| Command | Description |
|---------|-------------|
| `:DuckDB <query>` | Execute SQL query on buffer |
| `:DuckDBSchema [buffer]` | Show buffer schema |
| `:DuckDBBuffers` | List queryable buffers |
| `:DuckDBValidate [buffer]` | Validate CSV/JSON with inline diagnostics |
| `:DuckDBScratch` | Open persistent SQL scratch buffer |
| `:DuckDBHistory` | Browse and re-run query history |
| `:DuckDBSummary [buffer]` | Show SUMMARIZE statistics |
| `:DuckDBHover` | Show column stats under cursor |
| `:DuckDBConvert <format> [path]` | Convert to csv/json/jsonl/parquet |
| `:DuckDBPost <url>` | POST query results to URL |
| `:DuckDBTransform [query]` | Transform with diff preview |

### Default Keymaps

#### Global Keymaps

| Key | Mode | Description |
|-----|------|-------------|
| `<leader>dq` | n | Query prompt |
| `<leader>dq` | v | Execute visual selection |
| `<leader>ds` | n | Show schema |
| `<leader>dv` | n | Validate buffer |
| `<leader>db` | n | List buffers |
| `<leader>dp` | n | Preview (LIMIT 100) |
| `<leader>d1` | n | Preview (LIMIT 10) |
| `<leader>d5` | n | Preview (LIMIT 50) |
| `<leader>da` | n | Select all |
| `<leader>dn` | n | Count rows |
| `<leader>dh` | n | Query history |
| `<leader>dS` | n | Open scratch buffer |
| `<leader>du` | n | Show summary stats |
| `<leader>dt` | n | Transform with diff |
| `K` | n | Column stats (CSV/JSON buffers) |

#### Result Buffer Keymaps

| Key | Description |
|-----|-------------|
| `f` | Filter (add WHERE clause) |
| `s` | Sort by column under cursor |
| `ya` | Yank as JSON array |
| `yo` | Yank JSON (choose format) |
| `yc` | Yank as CSV |
| `e` | Export to file |
| `r` | Re-run query |
| `q` / `<Esc>` | Close window |

#### Scratch Buffer Keymaps

| Key | Description |
|-----|-------------|
| `<CR>` | Execute statement at cursor |
| `<C-CR>` | Execute statement at cursor |

### Configuration

```lua
require('duckdb').setup({
  -- Display
  max_rows = 1000,           -- Maximum rows to display
  max_col_width = 50,        -- Maximum column width
  auto_close = false,        -- Auto-close result window
  default_format = 'table',  -- Export format: 'csv', 'json', 'table'

  -- History
  history_limit = 500,       -- Maximum history entries

  -- Scratch buffer
  scratch_path = nil,        -- Custom path (nil = stdpath('cache')/duckdb_scratch.sql)

  -- Hover/inline features
  hover_stats = true,        -- Enable K mapping for column stats
  inline_preview = true,     -- Enable inline result previews
  inline_preview_debounce_ms = 500,
})
```

### Lua API

#### Core Functions

```lua
local duckdb = require('duckdb')

-- Execute query with options
duckdb.query(query, {
  buffer = nil,        -- Buffer identifier (number, name, or nil)
  display = 'float',   -- 'float', 'split', or 'none'
  export = nil,        -- Export file path
  format = nil,        -- Export format
  title = nil,         -- Window title
  skip_history = false -- Don't add to history
})

-- Get results as Lua table
local rows, err = duckdb.query_as_table('SELECT * FROM buffer')
for _, row in ipairs(rows) do
  print(row.name, row.salary)
end

-- Interactive query prompt
duckdb.query_prompt()

-- Execute visual selection
duckdb.query_visual()
```

#### Schema and Validation

```lua
-- Get schema
duckdb.get_schema(buffer_id)

-- List queryable buffers
local buffers = duckdb.list_queryable_buffers()

-- Validate buffer
local result, err = duckdb.validate(buffer_id, {
  show_diagnostics = true,
  show_float = true
})

-- Clear validation diagnostics
duckdb.clear_validation(buffer_id)
```

#### History and Scratch

```lua
-- Show history picker
duckdb.history()

-- Clear history
duckdb.clear_history()

-- Open scratch buffer
duckdb.scratch()
```

#### Statistics and Hover

```lua
-- Show SUMMARIZE stats
duckdb.summary(buffer_id)

-- Show column stats popup
duckdb.hover(buffer_id, column_name)
```

#### Export and Transform

```lua
-- Convert format
duckdb.convert('parquet', '/tmp/output.parquet')

-- POST to URL (from result buffer)
duckdb.post('https://api.example.com/data')

-- Transform with diff preview
duckdb.transform('SELECT * FROM buffer WHERE active = true')
```

#### History Module

```lua
local history = require('duckdb.history')

-- Add entry
history.add({
  query = 'SELECT * FROM buffer',
  timestamp = os.time(),
  row_count = 100,
  execution_time_ms = 50,
  buffer_name = 'data.csv'
})

-- Get entries
local entries = history.get({ limit = 10, search = 'SELECT' })

-- Search
local results = history.search('users', 20)

-- Recent queries
local recent = history.recent(5)

-- Clear all
history.clear()
```

#### Actions Module

```lua
local actions = require('duckdb.actions')

-- Format result as JSON
local json_array = actions.format_json_array(result)
local json_object = actions.format_json_object(result)
local json_single = actions.format_json_single(result)
local csv = actions.format_csv(result)
```

### QueryResult Object

```lua
{
  columns = {'name', 'age'},  -- Column names
  rows = {{'Alice', 30}},     -- Row data
  row_count = 1,              -- Number of rows
  column_count = 2,           -- Number of columns
  rows_changed = 0            -- For DML statements
}
```

### ValidationResult Object

```lua
{
  valid = true,
  errors = {
    { line = 3, column = 5, message = 'Invalid value', severity = 'error' }
  },
  warnings = {
    { line = 7, message = 'Inconsistent column count', severity = 'warning' }
  }
}
```

---

## Installation

### Requirements

- Neovim 0.7+ (includes LuaJIT)
- DuckDB 0.9.0+ shared library (`libduckdb`)

### Installing DuckDB

**macOS:**
```bash
brew install duckdb
```

**Ubuntu/Debian:**
```bash
sudo apt install libduckdb-dev
```

**Arch Linux:**
```bash
sudo pacman -S duckdb
```

**From Source:**
Download from [DuckDB Downloads](https://duckdb.org/docs/installation/)

### Plugin Installation

**lazy.nvim:**
```lua
{
  'hello-world-bfree/nvim-duckdb',
  config = function()
    require('duckdb').setup()
  end
}
```

**packer.nvim:**
```lua
use {
  'hello-world-bfree/nvim-duckdb',
  config = function()
    require('duckdb').setup()
  end
}
```

**vim-plug:**
```vim
Plug 'hello-world-bfree/nvim-duckdb'

lua << EOF
require('duckdb').setup()
EOF
```

### Verify Installation

```vim
:checkhealth duckdb
```

---

## Troubleshooting

### "DuckDB library not found"

Ensure `libduckdb` is installed and accessible:
```bash
ldconfig -p | grep duckdb  # Linux
ls /usr/local/lib | grep duckdb  # macOS
```

### "This plugin requires LuaJIT"

Use Neovim, not Vim. Neovim includes LuaJIT by default.

### "Query failed: Catalog Error"

Check table references:
- `buffer` for current buffer
- `buffer('name')` for named buffers
- `buffer(5)` for buffer number

### CSV not parsing correctly

DuckDB uses `read_csv_auto` with auto-detection. For edge cases, use `:DuckDBValidate` to identify issues.

### History not persisting

History is stored in `stdpath('data')/duckdb_history.json`. Check write permissions.

---

## See Also

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [LuaJIT FFI Tutorial](http://luajit.org/ext_ffi_tutorial.html)
- [Neovim Lua Guide](https://neovim.io/doc/user/lua-guide.html)
