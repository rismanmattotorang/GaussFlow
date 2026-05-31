#!/bin/bash

# GaussFlow End-to-End Build Script
# This script builds the entire GaussFlow project workspace

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse command line arguments
CLEAN=false
RELEASE=false
TEST=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN=true
            shift
            ;;
        --release)
            RELEASE=true
            shift
            ;;
        --test)
            TEST=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --clean      Clean build artifacts before building"
            echo "  --release    Build in release mode (optimized)"
            echo "  --test       Run tests after building"
            echo "  --verbose    Enable verbose output"
            echo "  --help       Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build mode
BUILD_MODE="dev"
if [ "$RELEASE" = true ]; then
    BUILD_MODE="release"
fi

print_info "Starting GaussFlow build process..."
print_info "Build mode: $BUILD_MODE"
echo ""

# Step 1: Clean (if requested)
if [ "$CLEAN" = true ]; then
    print_info "Cleaning build artifacts..."
    cargo clean
    print_success "Clean completed"
    echo ""
fi

# Step 2: Check Rust toolchain
print_info "Checking Rust toolchain..."
if ! command -v rustc &> /dev/null; then
    print_error "Rust is not installed or not in PATH"
    exit 1
fi

RUST_VERSION=$(rustc --version)
print_info "Rust version: $RUST_VERSION"
echo ""

# Step 3: Update dependencies (optional but recommended)
print_info "Updating dependencies..."
if [ "$VERBOSE" = true ]; then
    cargo update
else
    cargo update --quiet 2>&1 | grep -v "^$" || true
fi
print_success "Dependencies updated"
echo ""

# Step 4: Build the workspace
print_info "Building GaussFlow workspace ($BUILD_MODE mode)..."
echo ""

if [ "$RELEASE" = true ]; then
    if [ "$VERBOSE" = true ]; then
        cargo build --release
    else
        cargo build --release --quiet
    fi
else
    if [ "$VERBOSE" = true ]; then
        cargo build
    else
        cargo build --quiet
    fi
fi

if [ $? -eq 0 ]; then
    print_success "Build completed successfully!"
    echo ""
    
    # Show build artifacts
    print_info "Build artifacts location:"
    if [ "$RELEASE" = true ]; then
        TARGET_DIR="target/release"
    else
        TARGET_DIR="target/debug"
    fi
    
    echo "  - gaussflow-cli:   $TARGET_DIR/gaussflow-cli"
    echo "  - gaussflow-web:   $TARGET_DIR/gaussflow-web"
    echo "  - gaussflow-tui:   $TARGET_DIR/gaussflow-tui"
    echo ""
else
    print_error "Build failed!"
    exit 1
fi

# Step 5: Run tests (if requested)
if [ "$TEST" = true ]; then
    print_info "Running tests..."
    echo ""
    
    if [ "$VERBOSE" = true ]; then
        cargo test --release
    else
        cargo test --release --quiet
    fi
    
    if [ $? -eq 0 ]; then
        print_success "All tests passed!"
        echo ""
    else
        print_error "Tests failed!"
        exit 1
    fi
fi

# Step 6: Summary
print_success "=========================================="
print_success "GaussFlow build completed successfully!"
print_success "=========================================="
echo ""

# Show workspace members
print_info "Workspace members built:"
echo "  ✓ gaussflow-core"
echo "  ✓ gaussflow-runtime"
echo "  ✓ gaussflow-cli"
echo "  ✓ gaussflow-py"
echo "  ✓ gaussflow-web"
echo "  ✓ gaussflow-tui"
echo ""

print_info "To run the applications:"
if [ "$RELEASE" = true ]; then
    echo "  ./target/release/gaussflow-cli"
    echo "  ./target/release/gaussflow-web"
    echo "  ./target/release/gaussflow-tui"
else
    echo "  ./target/debug/gaussflow-cli"
    echo "  ./target/debug/gaussflow-web"
    echo "  ./target/debug/gaussflow-tui"
fi
