cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME inferal_relay)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# DuckDB's bundled httplib (for HTTP requests)
include_directories(${CMAKE_SOURCE_DIR}/third_party/httplib)

# OpenSSL for HTTPS support in httplib
find_package(OpenSSL QUIET)
if(OPENSSL_FOUND)
    include_directories(${OPENSSL_INCLUDE_DIR})
    add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)
    message(STATUS "inferal_relay: OpenSSL found at ${OPENSSL_ROOT_DIR} - HTTPS enabled")
else()
    message(WARNING "inferal_relay: OpenSSL not found - HTTPS disabled, only HTTP will work")
endif()

set(EXTENSION_SOURCES
    src/inferal_relay_extension.cpp
    src/schema.cpp
    src/secrets.cpp
    src/http.cpp
    src/url_builder.cpp
    src/parser.cpp
    src/context.cpp
    src/members.cpp
    src/sync.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL to extension targets
if(OPENSSL_FOUND)
    target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
