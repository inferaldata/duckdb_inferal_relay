#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace inferal_relay {

//! Register all sync-related functions: sync, sync_page, ingest_page, add_stream, remove_stream,
//! build_next_url, resolve_term, status, reset_cursor, ldes_read, version.
void RegisterFunctions(ExtensionLoader &loader);

} // namespace inferal_relay
} // namespace duckdb
