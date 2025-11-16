# nvim-duckdb Tests

This directory contains the test suite for nvim-duckdb.

## Test Structure

- `minimal_init.lua` - Minimal Neovim initialization for test environment
- `run_tests.sh` - Shell script to run all tests
- `plenary/` - Tests using plenary.nvim test framework
  - `ffi_cleanup_spec.lua` - Tests for FFI resource lifecycle management
  - `buffer_query_spec.lua` - Tests for buffer query functionality
  - `stress_test_spec.lua` - Stress tests for memory leaks and resource exhaustion

## Prerequisites

1. **Neovim** (v0.9.5 or later)
2. **DuckDB C library** (libduckdb.so)
3. **plenary.nvim** (auto-installed by test runner)

### Installing DuckDB Library

```bash
# Ubuntu/Debian
wget https://github.com/duckdb/duckdb/releases/download/v1.1.2/libduckdb-linux-amd64.zip
unzip libduckdb-linux-amd64.zip
sudo mv libduckdb.so /usr/local/lib/
sudo ldconfig

# macOS
brew install duckdb
```

## Running Tests

### Local Development

```bash
# Run all tests
./tests/run_tests.sh

# Or manually with Neovim
nvim --headless -u tests/minimal_init.lua \
  -c "PlenaryBustedDirectory tests/plenary/ {minimal_init = 'tests/minimal_init.lua', sequential = true}"
```

### GitHub Actions

Tests automatically run on:
- Pull requests to `main` or `master`
- Pushes to `main` or `master`

The CI pipeline tests against multiple Neovim versions (stable, v0.9.5, v0.10.0).

## Test Categories

### FFI Cleanup Tests

These tests verify the memory safety improvements:
- Double-close protection
- GC finalizer safety net
- Proper resource cleanup order
- Connection lifecycle management

### Buffer Query Tests

Tests for the buffer querying functionality:
- Connection management in query_buffer
- VARCHAR memory safety
- NULL handling
- Large string handling

### Stress Tests

Tests for resource exhaustion and memory leaks:
- Memory leak detection over many cycles
- Rapid GC during active connections
- Large result set handling
- Abandoned connection cleanup
- Error recovery patterns

## Writing New Tests

Tests use the plenary.nvim BDD-style syntax:

```lua
describe('Feature Name', function()
  before_each(function()
    -- Setup
  end)

  it('should do something', function()
    assert.equals(expected, actual)
  end)
end)
```

If DuckDB library is not available, tests will be skipped with `pending()`.
