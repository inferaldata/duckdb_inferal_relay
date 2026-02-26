#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
namespace inferal_relay {

//! Upsert a JSON-LD @context for a stream. Returns context_id or -1 if no processable context.
int64_t UpsertContext(ClientContext &context, const string &stream_name, const string &context_json);
int64_t UpsertContext(Connection &con, ClientContext &context, const string &stream_name, const string &context_json);

//! Resolve a compact IRI or bare term to a full IRI using a stream's latest (or specified) context.
string ResolveTerm(ClientContext &context, const string &stream_name, const string &term,
                   int64_t context_id = -1);

} // namespace inferal_relay
} // namespace duckdb
