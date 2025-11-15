# DuckDB.nvim

A Neovim plugin that integrates DuckDB via LuaJIT FFI to enable SQL queries on CSV and JSON files directly from buffers.

## Features

- **Direct Buffer Queries**: Query CSV and JSON data directly from Neovim buffers without saving files
- **Multiple Format Support**: CSV, JSON, and JSONL (newline-delimited JSON)
- **Multi-Buffer Joins**: Query and join data across multiple buffers
- **Interactive UI**: Beautiful floating window results with proper formatting
- **Export Capabilities**: Export results to CSV, JSON, or formatted tables
- **Schema Inspection**: Inspect buffer schemas with `:DuckDBSchema`
- **Auto-Detection**: Automatically detects file types from filetype or extension
- **Full SQL Support**: Leverage DuckDB's complete SQL capabilities
- **Smart Validation**: Leverage DuckDB's excellent parser to identify and pinpoint CSV/JSON errors with inline diagnostics

## Requirements

- Neovim 0.7+ (includes LuaJIT by default)
- DuckDB shared library (`libduckdb`)

### Installing DuckDB

**Ubuntu/Debian:**
```bash
sudo apt install libduckdb-dev
```

**macOS:**
```bash
brew install duckdb
```

**Arch Linux:**
```bash
sudo pacman -S duckdb
```

**From Source:**
Download from [DuckDB Downloads](https://duckdb.org/docs/installation/)

## Installation

### Using [lazy.nvim](https://github.com/folke/lazy.nvim)

```lua
{
  'yourusername/duckdb.nvim',
  config = function()
    require('duckdb').setup({
      max_rows = 1000,        -- Maximum rows to display
      max_col_width = 50,     -- Maximum column width
      auto_close = false,     -- Auto-close result window
      default_format = 'table' -- Default export format
    })
  end
}
```

### Using [packer.nvim](https://github.com/wbthomason/packer.nvim)

```lua
use {
  'yourusername/duckdb.nvim',
  config = function()
    require('duckdb').setup()
  end
}
```

### Using [vim-plug](https://github.com/junegunn/vim-plug)

```vim
Plug 'yourusername/duckdb.nvim'

lua << EOF
require('duckdb').setup()
EOF
```

## Usage

### Commands

#### `:DuckDB <query>`

Execute a SQL query on the current buffer.

```vim
:DuckDB SELECT * FROM buffer WHERE age > 25 ORDER BY name

:DuckDB SELECT COUNT(*) as total, AVG(price) as avg_price FROM buffer
```

#### Visual Selection

Select multiple lines containing a SQL query and execute with `:DuckDB` or the visual mode keymap.

#### `:DuckDBSchema`

Show the schema of the current buffer or a specific buffer.

```vim
:DuckDBSchema
:DuckDBSchema 5
:DuckDBSchema data.csv
```

#### `:DuckDBBuffers`

List all queryable buffers (CSV, JSON, JSONL).

#### `:DuckDBValidate [buffer]`

Validate CSV, JSON, or JSONL content using DuckDB's parser. This leverages DuckDB's excellent error reporting to identify issues like:
- Inconsistent column counts in CSV files
- Malformed JSON syntax
- Type mismatches
- Encoding issues

The command will:
- Display errors and warnings in a floating window
- Show inline diagnostics with line numbers
- Pinpoint the exact location of parsing errors

```vim
:DuckDBValidate               " Validate current buffer
:DuckDBValidate 5            " Validate buffer 5
:DuckDBValidate data.csv     " Validate specific buffer
```

#### `:DuckDBClearValidation [buffer]`

Clear validation diagnostics from a buffer.

```vim
:DuckDBClearValidation        " Clear diagnostics from current buffer
```

### Default Keymaps

- `<leader>dq` (normal mode): Open query prompt
- `<leader>dq` (visual mode): Execute selected query
- `<leader>ds`: Show schema of current buffer
- `<leader>dv`: Validate current buffer

Disable default keymaps in your config:

```lua
vim.g.duckdb_no_default_keymaps = 1
```

### Examples

#### Querying a CSV Buffer

Given a CSV file `employees.csv`:
```csv
name,age,department,salary
Alice,30,Engineering,95000
Bob,25,Marketing,65000
Charlie,35,Engineering,105000
Diana,28,Sales,70000
```

Open in Neovim and query:
```vim
:DuckDB SELECT department, COUNT(*) as count, AVG(salary) as avg_salary FROM buffer GROUP BY department
```

Result:
```
┌──────────────┬───────┬────────────┐
│ department   │ count │ avg_salary │
├──────────────┼───────┼────────────┤
│ Engineering  │ 2     │ 100000.0   │
│ Marketing    │ 1     │ 65000.0    │
│ Sales        │ 1     │ 70000.0    │
└──────────────┴───────┴────────────┘

3 row(s) returned
```

#### Querying JSON Data

Given a JSON file `products.json`:
```json
[
  {"id": 1, "name": "Laptop", "price": 999.99, "category": "Electronics"},
  {"id": 2, "name": "Mouse", "price": 24.99, "category": "Electronics"},
  {"id": 3, "name": "Desk", "price": 299.99, "category": "Furniture"}
]
```

Query:
```vim
:DuckDB SELECT category, COUNT(*) as items, SUM(price) as total FROM buffer GROUP BY category
```

#### Joining Multiple Buffers

Open two buffers: `orders.csv` and `customers.json`

```vim
:DuckDB SELECT c.name, COUNT(o.id) as order_count FROM buffer('customers.json') c JOIN buffer('orders.csv') o ON c.id = o.customer_id GROUP BY c.name
```

#### Using Lua API

```lua
local duckdb = require('duckdb')

-- Execute query and get results
local result, err = duckdb.query('SELECT * FROM buffer WHERE price > 50')

if result then
  print(string.format("Found %d rows", result.row_count))
end

-- Get results as Lua table
local rows = duckdb.query_as_table('SELECT * FROM buffer LIMIT 10')
for _, row in ipairs(rows) do
  print(row.name, row.age)
end

-- Export results
duckdb.query('SELECT * FROM buffer', {
  export = '/tmp/results.csv',
  format = 'csv'
})

-- Query specific buffer
duckdb.query('SELECT * FROM buffer', {
  buffer = 5  -- buffer number
})

-- Display in split instead of float
duckdb.query('SELECT * FROM buffer', {
  display = 'split'
})
```

## Advanced Features

### Complex SQL Queries

DuckDB supports advanced SQL features:

```sql
-- Window functions
SELECT name, salary,
       AVG(salary) OVER (PARTITION BY department) as dept_avg
FROM buffer

-- CTEs (Common Table Expressions)
WITH high_earners AS (
  SELECT * FROM buffer WHERE salary > 80000
)
SELECT department, COUNT(*) FROM high_earners GROUP BY department

-- Subqueries
SELECT * FROM buffer
WHERE salary > (SELECT AVG(salary) FROM buffer)

-- JSON path extraction
SELECT data->>'$.name' as name FROM buffer
```

### Working with JSONL

For large JSON datasets, use JSONL (newline-delimited JSON):

```jsonl
{"id": 1, "name": "Alice", "score": 95}
{"id": 2, "name": "Bob", "score": 87}
{"id": 3, "name": "Charlie", "score": 92}
```

```vim
:DuckDB SELECT name, score FROM buffer WHERE score > 90 ORDER BY score DESC
```

### Headerless CSV

DuckDB auto-detects CSV headers. For headerless CSVs, the plugin will use generated column names.

### Data Validation

The plugin leverages DuckDB's excellent CSV and JSON parser to validate your data files and pinpoint errors with precision.

#### Why Use DuckDB for Validation?

DuckDB's parser is exceptionally good at identifying data issues:
- **Precise Error Location**: Identifies the exact line and column of errors
- **Detailed Error Messages**: Explains what went wrong and why
- **Format-Specific Validation**: Understands CSV structure, JSON syntax, and type constraints
- **Performance**: Fast validation even on large files

#### Validating CSV Files

Open a CSV file and run:

```vim
:DuckDBValidate
```

The plugin will detect issues like:
- **Inconsistent column counts**: Rows with too few or too many columns
- **Type mismatches**: Non-numeric values in numeric columns
- **Encoding problems**: Binary or control characters in text
- **Quote handling errors**: Unclosed quotes or improper escaping

Example with `examples/invalid_csv.csv`:
```csv
name,age,department,salary
Alice,30,Engineering,95000
Bob,25,Marketing           ← Missing column
Charlie,35,Engineering,105000,extra_field  ← Extra column
```

Running `:DuckDBValidate` will show:
```
Validation Results: invalid_csv.csv
════════════════════════════════════════════════════════════

✗ Errors: 0
⚠ Warnings: 2

CSV with 8 lines, 0 errors, 2 warnings

Warnings:
────────────────────────────────────────────────────────────
1. [schema] Line 3: Inconsistent column count: expected 4 columns, found 3
2. [schema] Line 4: Inconsistent column count: expected 4 columns, found 5
```

#### Validating JSON Files

For JSON files, the validator checks:
- **Syntax errors**: Missing commas, brackets, or quotes
- **Structure issues**: Invalid nesting or malformed objects
- **Schema validation**: Whether the JSON is an array (required for queries)

Example with `examples/invalid_json.json`:
```json
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "name": "Bob",},     ← Trailing comma
  {
    "id": 3                       ← Missing comma
    "name": "Charlie"
  }
]
```

The validator will show the exact line number and error description.

#### Validating JSONL Files

For JSONL (newline-delimited JSON), each line is validated individually:

```vim
:DuckDBValidate
```

Issues detected:
- **Invalid JSON on specific lines**: Shows which line has the error
- **Type inconsistencies**: Mixed types across lines
- **Malformed objects**: Missing keys or values

Example with `examples/invalid_jsonl.jsonl`:
```jsonl
{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"        ← Missing closing brace
{"id": 3 "name": "Charlie"}    ← Missing comma
```

#### Inline Diagnostics

Validation errors appear as inline diagnostics in Neovim, similar to LSP errors:
- Error messages show in the sign column
- Hover over highlighted lines to see details
- Navigate with `:lnext` and `:lprev` or your diagnostic navigation keys

#### Using Validation in Lua

```lua
local duckdb = require('duckdb')

-- Validate current buffer
local result, err = duckdb.validate()

if result then
  print(string.format("Errors: %d, Warnings: %d",
    #result.errors, #result.warnings))

  for _, error in ipairs(result.errors) do
    print(string.format("Line %d: %s", error.line, error.message))
  end
end

-- Validate specific buffer
duckdb.validate('data.csv')

-- Validate without showing UI (programmatic validation)
duckdb.validate(nil, { show_float = false, show_diagnostics = true })

-- Clear diagnostics
duckdb.clear_validation()
```

### Export Results

```lua
require('duckdb').query('SELECT * FROM buffer WHERE amount > 1000', {
  export = '/tmp/filtered.csv',
  format = 'csv'
})

require('duckdb').query('SELECT * FROM buffer', {
  export = '/tmp/results.json',
  format = 'json'
})
```

## Health Check

Run health check to verify installation:

```vim
:checkhealth duckdb
```

This will verify:
- LuaJIT availability
- FFI module
- DuckDB library
- Basic query functionality
- CSV/JSON parsing

## Configuration

Full configuration options:

```lua
require('duckdb').setup({
  -- Maximum number of rows to display in results
  max_rows = 1000,

  -- Maximum column width in display
  max_col_width = 50,

  -- Auto-close result window on selection
  auto_close = false,

  -- Default export format: 'csv', 'json', or 'table'
  default_format = 'table',
})
```

## API Reference

### `setup(opts)`

Initialize the plugin with configuration options.

### `query(query, opts)`

Execute a SQL query.

**Parameters:**
- `query` (string): SQL query to execute
- `opts` (table, optional):
  - `buffer`: Buffer identifier (number, name, or nil for current)
  - `display`: Display mode (`"float"`, `"split"`, or `"none"`)
  - `export`: Export file path
  - `format`: Export format (`"csv"`, `"json"`, or `"table"`)
  - `title`: Custom window title

**Returns:**
- `result`: QueryResult object or nil
- `error`: Error message or nil

### `query_as_table(query, buffer_id)`

Execute query and return results as Lua table of objects.

**Returns:**
- `rows`: Array of row objects with column names as keys
- `error`: Error message or nil

### `get_schema(identifier)`

Get schema information for a buffer.

### `list_queryable_buffers()`

Get list of all buffers that can be queried.

### `validate(identifier, opts)`

Validate buffer content using DuckDB's parser.

**Parameters:**
- `identifier` (string|number|nil): Buffer identifier (nil for current buffer)
- `opts` (table, optional):
  - `show_diagnostics`: Show inline diagnostics (default: true)
  - `show_float`: Show floating window with results (default: true)

**Returns:**
- `result`: ValidationResult object with `errors`, `warnings`, and `valid` fields
- `error`: Error message or nil

### `validate_current_buffer(opts)`

Validate the current buffer. Shorthand for `validate(nil, opts)`.

### `clear_validation(identifier)`

Clear validation diagnostics from a buffer.

**Parameters:**
- `identifier` (string|number|nil): Buffer identifier (nil for current buffer)

## Troubleshooting

### "DuckDB library not found"

Make sure `libduckdb` is installed and accessible. Check with:
```bash
ldconfig -p | grep duckdb  # Linux
ls /usr/local/lib | grep duckdb  # macOS
```

### "This plugin requires LuaJIT"

You must use Neovim (not Vim), as Neovim includes LuaJIT by default.

### "Query failed: Catalog Error"

Make sure your SQL query references the correct table name:
- Use `buffer` for the current buffer
- Use `buffer('name')` for named buffers
- Use `buffer(5)` for buffer number 5

### CSV not parsing correctly

The plugin uses DuckDB's `read_csv_auto` which auto-detects delimiters and headers. For edge cases, you may need to save the file and use DuckDB's explicit CSV options.

## Performance

- Queries are executed in-memory for maximum performance
- Large buffers (>100MB) may take time to parse
- Use `LIMIT` clauses for initial exploration
- The `max_rows` config limits display but doesn't affect query execution

## Architecture

```
plugin/duckdb.lua          -- Plugin initialization, commands, keymaps
lua/duckdb/
  init.lua                 -- Main module, public API
  ffi.lua                  -- DuckDB FFI bindings
  buffer.lua               -- Buffer content extraction
  query.lua                -- Query execution engine
  ui.lua                   -- Result display and formatting
  validate.lua             -- Data validation with error reporting
  health.lua               -- Health checks
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Credits

- Built with [DuckDB](https://duckdb.org/) - an in-process analytical database
- Inspired by Neovim's extensibility and LuaJIT's FFI capabilities

## See Also

- [DuckDB Documentation](https://duckdb.org/docs/)
- [LuaJIT FFI Tutorial](http://luajit.org/ext_ffi_tutorial.html)
- [Neovim Lua Guide](https://neovim.io/doc/user/lua-guide.html)
