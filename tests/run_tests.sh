#!/bin/bash
set -e

# Test runner script for nvim-duckdb
# This script sets up the environment and runs all tests

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PLENARY_DIR="${PLENARY_DIR:-/tmp/plenary.nvim}"

echo "=== nvim-duckdb Test Runner ==="
echo "Project root: $PROJECT_ROOT"
echo "Plenary dir: $PLENARY_DIR"

# Check for Neovim
if ! command -v nvim &> /dev/null; then
    echo "ERROR: Neovim not found in PATH"
    exit 1
fi

NVIM_VERSION=$(nvim --version | head -1)
echo "Neovim version: $NVIM_VERSION"

# Install plenary.nvim if not present
if [ ! -d "$PLENARY_DIR" ]; then
    echo "Installing plenary.nvim..."
    git clone --depth 1 https://github.com/nvim-lua/plenary.nvim "$PLENARY_DIR"
fi

# Check for DuckDB library
echo "Checking for DuckDB library..."
if ldconfig -p 2>/dev/null | grep -q libduckdb; then
    echo "DuckDB library found via ldconfig"
elif [ -f /usr/lib/libduckdb.so ] || [ -f /usr/local/lib/libduckdb.so ]; then
    echo "DuckDB library found in standard location"
else
    echo "WARNING: DuckDB library may not be installed. Some tests will be skipped."
fi

# Export plenary directory for tests
export PLENARY_DIR

# Run plenary tests
echo ""
echo "=== Running Plenary Tests ==="
nvim --headless \
    -u "$SCRIPT_DIR/minimal_init.lua" \
    -c "PlenaryBustedDirectory $SCRIPT_DIR/plenary/ {minimal_init = '$SCRIPT_DIR/minimal_init.lua', sequential = true}" \
    2>&1

TEST_EXIT_CODE=$?

echo ""
echo "=== Test Run Complete ==="

exit $TEST_EXIT_CODE
