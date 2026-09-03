# CMakeLists.txt for Thryve FFI example application
#
# Usage:
#   mkdir build && cd build
#   cmake ..
#   make
#
# For Windows DLL/LIB distribution:
#   The cargo build produces:
#     - thryve_ffi.dll (shared library) in target/debug/ or target/release/
#     - thryve_ffi.lib (import library) in target/debug/ or target/release/
#     - thryve_ffi.pdb (debug symbols) in target/debug/ or target/release/
#
#   You need to:
#   1. Copy thryve_ffi.dll to your application's output directory
#   2. Link against thryve_ffi.lib during build
#   3. The headers (thryve.h and thryve_ffi.h) are generated in OUT_DIR
#      and also copied to thryve_ffi/out/ for IDE support
#
#   Alternatively, set these paths via -D on the cmake command line:
#     THRYVE_FFI_OUT_DIR — Directory containing headers (OUT_DIR from cargo build)
#     THRYVE_FFI_LIB_DIR — Directory containing the library files (.lib/.dll or .so/.a)
#     THRYVE_FFI_INCLUDE_DIR — Directory containing thryve.h and thryve_ffi.h

cmake_minimum_required(VERSION 3.15)
project(thryve_ffi_example LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

# ============================================================
# Find the Thryve FFI library and headers
# ============================================================
#
# Set these paths via -D on the cmake command line, or use the defaults below:
#
#   THRYVE_FFI_DIR — Root of thryve_ffi build (contains Cargo.toml)
#   THRYVE_FFI_OUT_DIR — OUT_DIR from cargo build (contains headers + lib)
#   THRYVE_FFI_STABLE_DIR — Stable output directory (thryve_ffi/out/, contains headers)
#   THRYVE_FFI_LIB_DIR — Directory containing the .lib/.dll files
#
# Default: use cargo to find the OUT_DIR and stable headers from thryve_ffi/out/
find_program(CARGO cargo REQUIRED)

# Get OUT_DIR from cargo (for library files)
execute_process(
    COMMAND ${CARGO} rustc --package thryve_ffi -- -Z unstable-options --print out-dir
    OUTPUT_VARIABLE THRYVE_FFI_OUT_DIR_RAW
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Allow overriding OUT_DIR via command line
set(THRYVE_FFI_OUT_DIR ${THRYVE_FFI_OUT_DIR_RAW} CACHE PATH "OUT_DIR from cargo build")

# Use stable output directory for headers (thryve_ffi/out/)
# This is where build.rs copies the headers for IDE support
set(THRYVE_FFI_STABLE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/out" CACHE PATH "Stable header output directory")

# Determine the lib directory based on platform and build type
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(THRYVE_FFI_LIB_SUBDIR "release")
else()
    set(THRYVE_FFI_LIB_SUBDIR "debug")
endif()

# On Windows, the lib files are in OUT_DIR, not OUT_DIR/target
# On Unix, they might be in OUT_DIR/target/<arch>/release or OUT_DIR/release
set(THRYVE_FFI_LIB_DIR "${THRYVE_FFI_OUT_DIR}/${THRYVE_FFI_LIB_SUBDIR}")
set(THRYVE_FFI_INCLUDE_DIR "${THRYVE_FFI_STABLE_DIR}")

# ============================================================
# Define the Thryve FFI target
# ============================================================

# For Windows DLL
if(WIN32)
    set(THRYVE_FFI_LIB_NAME "thryve_ffi")
    set(THRYVE_FFI_DLL "${THRYVE_FFI_LIB_DIR}/thryve_ffi.dll")
    set(THRYVE_FFI_IMPORT_LIB "${THRYVE_FFI_LIB_DIR}/thryve_ffi.lib")
    
    # Create a library target for the import library
    if(EXISTS "${THRYVE_FFI_IMPORT_LIB}")
        add_library(thryve_ffi SHARED IMPORTED)
        set_target_properties(thryve_ffi PROPERTIES
            IMPORTED_LOCATION "${THRYVE_FFI_DLL}"
            IMPORTED_IMPLIB "${THRYVE_FFI_IMPORT_LIB}"
        )
    else()
        # If import library doesn't exist yet, warn user
        message(WARNING "thryve_ffi.lib not found at ${THRYVE_FFI_IMPORT_LIB}. 
                         Make sure you've run 'cargo build --package thryve_ffi' first.")
    endif()
elseif(APPLE)
    set(THRYVE_FFI_LIB_NAME "libthryve_ffi.dylib")
    set(THRYVE_FFI_DYLIB "${THRYVE_FFI_LIB_DIR}/libthryve_ffi.dylib")
    add_library(thryve_ffi SHARED IMPORTED)
    set_target_properties(thryve_ffi PROPERTIES
        IMPORTED_LOCATION "${THRYVE_FFI_DYLIB}"
    )
else()
    set(THRYVE_FFI_LIB_NAME "libthryve_ffi.so")
    set(THRYVE_FFI_SO "${THRYVE_FFI_LIB_DIR}/libthryve_ffi.so")
    add_library(thryve_ffi SHARED IMPORTED)
    set_target_properties(thryve_ffi PROPERTIES
        IMPORTED_LOCATION "${THRYVE_FFI_SO}"
    )
endif()

# ============================================================
# Copy DLL to output directory (Windows)
# ============================================================
if(WIN32 AND EXISTS "${THRYVE_FFI_DLL}")
    # Copy the DLL next to the executable so it can be found
    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${THRYVE_FFI_DLL}"
            $<TARGET_FILE_DIR:thryve_ffi_example_cpp>
        COMMENT "Copying thryve_ffi.dll to output directory"
    )
endif()

# ============================================================
# Example C executable
# ============================================================
add_executable(thryve_ffi_example_c examples/main.c)
target_include_directories(thryve_ffi_example_c PRIVATE ${THRYVE_FFI_INCLUDE_DIR})
target_link_libraries(thryve_ffi_example_c PRIVATE thryve_ffi)

# ============================================================
# Example C++ executable - uses the extern "C" wrapper header
# ============================================================
add_executable(thryve_ffi_example_cpp examples/main.cpp)
target_include_directories(thryve_ffi_example_cpp PRIVATE ${THRYVE_FFI_INCLUDE_DIR})
target_link_libraries(thryve_ffi_example_cpp PRIVATE thryve_ffi)

# ============================================================
# Installation targets
# ============================================================
install(TARGETS thryve_ffi_example_c thryve_ffi_example_cpp
    RUNTIME DESTINATION bin
)

# On Windows, install the DLL and import library
if(WIN32)
    install(FILES ${THRYVE_FFI_DLL} ${THRYVE_FFI_IMPORT_LIB}
        DESTINATION lib
    )
endif()

# Install the headers
install(FILES ${THRYVE_FFI_INCLUDE_DIR}/thryve.h
        ${THRYVE_FFI_INCLUDE_DIR}/thryve_ffi.h
    DESTINATION include
)
