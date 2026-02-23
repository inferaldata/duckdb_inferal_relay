#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace inferal_relay {

//! Register the "inferal_relay" secret type with DuckDB's Secret Manager.
void RegisterSecretType(ExtensionLoader &loader);

//! Look up the API key for a given stream name.
//! Tries: exact match on stream name scope, then URL prefix match, then unscoped fallback.
//! Returns empty string if no secret found.
string LookupApiKey(ClientContext &context, const string &stream_name, const string &stream_url);

} // namespace inferal_relay
} // namespace duckdb
