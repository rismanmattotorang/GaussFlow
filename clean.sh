#!/bin/bash

# GaussFlow End-to-End Clean Script
# This script removes all build artifacts and temporary files from the project

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

# Function to safely remove directory
remove_dir() {
    if [ -d "$1" ]; then
        print_info "Removing $1..."
        rm -rf "$1"
        print_success "Removed $1"
    fi
}

# Function to safely remove file
remove_file() {
    if [ -f "$1" ]; then
        print_info "Removing $1..."
        rm -f "$1"
        print_success "Removed $1"
    fi
}

# Parse command line arguments
DEEP_CLEAN=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --deep)
            DEEP_CLEAN=true
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
            echo "  --deep      Perform deep clean (removes additional artifacts)"
            echo "  --verbose   Enable verbose output"
            echo "  --help      Show this help message"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

print_info "Starting GaussFlow clean process..."
echo ""

# Step 1: Clean Cargo build artifacts
print_info "Cleaning Cargo build artifacts..."
if [ "$VERBOSE" = true ]; then
    cargo clean
else
    cargo clean 2>&1 | grep -v "^$" || true
fi
print_success "Cargo artifacts cleaned"
echo ""

# Step 2: Remove target directory (if it still exists)
remove_dir "target"
echo ""

# Step 3: Remove Cargo.lock (optional, but can be regenerated)
if [ "$DEEP_CLEAN" = true ]; then
    print_info "Deep clean mode: Removing Cargo.lock..."
    remove_file "Cargo.lock"
    echo ""
fi

# Step 4: Clean Python build artifacts (for gaussflow-py)
print_info "Cleaning Python build artifacts..."
if [ -d "gaussflow-py" ]; then
    # Remove Python __pycache__ directories
    find gaussflow-py -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find gaussflow-py -type f -name "*.pyc" -delete 2>/dev/null || true
    find gaussflow-py -type f -name "*.pyo" -delete 2>/dev/null || true
    
    # Remove Python build directories
    remove_dir "gaussflow-py/build"
    remove_dir "gaussflow-py/dist"
    remove_dir "gaussflow-py/*.egg-info"
    
    # Remove any .so files (Python extensions)
    find gaussflow-py -type f -name "*.so" -delete 2>/dev/null || true
    find gaussflow-py -type f -name "*.dylib" -delete 2>/dev/null || true
    find gaussflow-py -type f -name "*.dll" -delete 2>/dev/null || true
fi
print_success "Python artifacts cleaned"
echo ""

# Step 5: Remove editor/IDE artifacts (deep clean)
if [ "$DEEP_CLEAN" = true ]; then
    print_info "Deep clean mode: Removing editor/IDE artifacts..."
    
    # Remove common editor directories
    remove_dir ".vscode"
    remove_dir ".idea"
    remove_dir "*.swp"
    remove_dir "*.swo"
    remove_dir "*~"
    
    # Remove backup files
    find . -type f -name "*.bak" -delete 2>/dev/null || true
    find . -type f -name "*.tmp" -delete 2>/dev/null || true
    find . -type f -name ".DS_Store" -delete 2>/dev/null || true
    
    print_success "Editor artifacts cleaned"
    echo ""
fi

# Step 6: Remove log files
print_info "Cleaning log files..."
find . -type f -name "*.log" -delete 2>/dev/null || true
print_success "Log files cleaned"
echo ""

# Step 7: Remove temporary Rust files
print_info "Cleaning temporary Rust files..."
find . -type f -name "*.pdb" -delete 2>/dev/null || true
find . -type f -name "*.rlib" -delete 2>/dev/null || true
print_success "Temporary Rust files cleaned"
echo ""

# Step 8: Summary
TOTAL_SIZE=0
if command -v du &> /dev/null; then
    # Calculate size before cleaning (approximate)
    if [ -d "target" ]; then
        TOTAL_SIZE=$(du -sh target 2>/dev/null | cut -f1 || echo "unknown")
    fi
fi

print_success "=========================================="
print_success "GaussFlow clean completed successfully!"
print_success "=========================================="
echo ""

print_info "Cleaned artifacts:"
echo "  ✓ Cargo build artifacts (target/)"
echo "  ✓ Python build artifacts"
echo "  ✓ Log files"
echo "  ✓ Temporary Rust files"

if [ "$DEEP_CLEAN" = true ]; then
    echo "  ✓ Cargo.lock"
    echo "  ✓ Editor/IDE artifacts"
fi

echo ""

print_info "The project is now clean and ready for a fresh build."
print_info "Run './build.sh' to rebuild the project."
