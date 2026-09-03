# Makefile for Thryve FFI example application
#
# Usage:
#   make build      - Build the FFI library (produces DLL + LIB on Windows)
#   make example    - Build the C++ example application
#   make example-c  - Build the C example application
#   make clean      - Remove build artifacts
#   make install    - Install headers, DLL, and LIB files
#
# Windows DLL/LIB Distribution:
#   After running 'make build', you'll have:
#     - target/debug/thryve_ffi.dll (shared library)
#     - target/debug/thryve_ffi.lib (import library for linking)
#     - target/debug/thryve_ffi.pdb (debug symbols)
#
#   For release builds:
#     cargo build --release --package thryve_ffi
#   This produces:
#     - target/release/thryve_ffi.dll
#     - target/release/thryve_ffi.lib

CARGO = cargo
CC = gcc
CXX = g++
RUSTFLAGS =

# Build configuration
BUILD_TYPE ?= debug

# Output directory
OUT_DIR = target/$(BUILD_TYPE)

# Header output directory
HEADER_DIR = $(OUT_DIR)

# Library paths
LIB_NAME = thryve_ffi
DLL_PATH = $(OUT_DIR)/$(LIB_NAME).dll
LIB_PATH = $(OUT_DIR)/$(LIB_NAME).lib
SO_PATH = $(OUT_DIR)/lib$(LIB_NAME).so

# Source files
EXAMPLE_C = examples/main.c
EXAMPLE_CPP = examples/main.cpp

# ============================================================
# Default target: build the library
# ============================================================
.PHONY: all
all: build

# ============================================================
# Build the FFI library (cargo build)
# ============================================================
.PHONY: build
build:
	$(CARGO) build $(RUSTFLAGS) --package thryve_ffi

# ============================================================
# Generate headers manually with cbindgen
# ============================================================
# Note: cbindgen must be installed first: cargo install cbindgen
.PHONY: headers
headers:
	cbindgen --manifest-path crates/thryve_ffi/Cargo.toml -o $(HEADER_DIR)/thryve.h

# ============================================================
# Build C++ example
# ============================================================
.PHONY: example
example: build $(LIB_PATH) headers
ifeq ($(OS),Windows_NT)
	$(CXX) -std=c++17 -I$(HEADER_DIR) -o thryve_example_cpp $(EXAMPLE_CPP) -L$(OUT_DIR) -l$(LIB_NAME)
	@echo ""
	@echo "=== Windows DLL/LIB Build Complete ==="
	@echo "DLL:  $(DLL_PATH)"
	@echo "LIB:  $(LIB_PATH)"
	@echo "Header: $(HEADER_DIR)/thryve_ffi.h"
	@echo ""
	@echo "To run: thryve_example_cpp.exe (ensure thryve_ffi.dll is in the same directory)"
else
	$(CXX) -std=c++17 -I$(HEADER_DIR) -o thryve_example_cpp $(EXAMPLE_CPP) -L$(OUT_DIR) -l$(LIB_NAME) -Wl,-rpath,$(OUT_DIR)
endif

# ============================================================
# Build C example
# ============================================================
.PHONY: example-c
example-c: build $(LIB_PATH) headers
ifeq ($(OS),Windows_NT)
	$(CC) -std=c11 -I$(HEADER_DIR) -o thryve_example_c $(EXAMPLE_C) -L$(OUT_DIR) -l$(LIB_NAME)
	@echo ""
	@echo "=== Windows DLL/LIB Build Complete ==="
	@echo "DLL:  $(DLL_PATH)"
	@echo "LIB:  $(LIB_PATH)"
	@echo "Header: $(HEADER_DIR)/thryve.h"
	@echo ""
	@echo "To run: thryve_example_c.exe (ensure thryve_ffi.dll is in the same directory)"
else
	$(CC) -std=c11 -I$(HEADER_DIR) -o thryve_example_c $(EXAMPLE_C) -L$(OUT_DIR) -l$(LIB_NAME) -Wl,-rpath,$(OUT_DIR)
endif

# ============================================================
# Install files (copy to specified directories)
# ============================================================
.PHONY: install
install: build $(LIB_PATH) headers
	@mkdir -p $(INSTALL_PREFIX)/lib
	@mkdir -p $(INSTALL_PREFIX)/include
ifeq ($(OS),Windows_NT)
	@cp $(DLL_PATH) $(INSTALL_PREFIX)/lib/
	@cp $(LIB_PATH) $(INSTALL_PREFIX)/lib/
endif
ifeq ($(UNAME_S),Linux)
	@cp $(SO_PATH) $(INSTALL_PREFIX)/lib/
endif
	@cp $(HEADER_DIR)/thryve.h $(INSTALL_PREFIX)/include/
	@cp $(HEADER_DIR)/thryve_ffi.h $(INSTALL_PREFIX)/include/
	@echo "Installed to $(INSTALL_PREFIX):"
	@echo "  $(INSTALL_PREFIX)/include/thryve.h"
	@echo "  $(INSTALL_PREFIX)/include/thryve_ffi.h"
ifeq ($(OS),Windows_NT)
	@echo "  $(INSTALL_PREFIX)/lib/thryve_ffi.dll"
	@echo "  $(INSTALL_PREFIX)/lib/thryve_ffi.lib"
endif
ifeq ($(UNAME_S),Linux)
	@echo "  $(INSTALL_PREFIX)/lib/libthryve_ffi.so"
endif

# ============================================================
# Release build
# ============================================================
.PHONY: release
release:
	$(CARGO) build --release $(RUSTFLAGS) --package thryve_ffi

.PHONY: example-release
example-release: release $(LIB_PATH) headers
ifeq ($(OS),Windows_NT)
	$(CXX) -std=c++17 -O2 -I$(HEADER_DIR) -o thryve_example_cpp $(EXAMPLE_CPP) -L$(OUT_DIR) -l$(LIB_NAME)
	@echo ""
	@echo "=== Windows Release Build Complete ==="
	@echo "DLL:  $(OUT_DIR)/release/$(LIB_NAME).dll"
	@echo "LIB:  $(OUT_DIR)/release/$(LIB_NAME).lib"
else
	$(CXX) -std=c++17 -O2 -I$(HEADER_DIR) -o thryve_example_cpp $(EXAMPLE_CPP) -L$(OUT_DIR) -l$(LIB_NAME) -Wl,-rpath,$(OUT_DIR)
endif

# ============================================================
# Clean
# ============================================================
.PHONY: clean
clean:
	$(CARGO) clean
	rm -f thryve_example_cpp thryve_example_c

# ============================================================
# Display help
# ============================================================
.PHONY: help
help:
	@echo "Thryve FFI Build System"
	@echo ""
	@echo "Targets:"
	@echo "  make build       - Build the FFI library (DLL + LIB on Windows)"
	@echo "  make example     - Build the C++ example application"
	@echo "  make example-c   - Build the C example application"
	@echo "  make headers     - Generate header files"
	@echo "  make release     - Build in release mode"
	@echo "  make install     - Install headers, DLL, and LIB files"
	@echo "  make clean       - Remove build artifacts"
	@echo "  make help        - Show this help"
	@echo ""
	@echo "Windows DLL/LIB Distribution:"
	@echo "  After building, you'll have:"
	@echo "    target/debug/thryve_ffi.dll   - Shared library (DLL)"
	@echo "    target/debug/thryve_ffi.lib   - Import library (for linking)"
	@echo "    target/debug/thryve_ffi.pdb   - Debug symbols"
	@echo ""
	@echo "  To distribute:"
	@echo "    1. Copy thryve_ffi.dll to your application directory"
	@echo "    2. Link against thryve_ffi.lib"
	@echo "    3. Include thryve_ffi.h (C++) or thryve.h (C)"
