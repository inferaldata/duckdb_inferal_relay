#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace inferal_relay {

//! Build the next URL for fetching a page from the LDES stream.
//! Reads stream config from inferal_relay.streams.
string BuildNextUrl(ClientContext &context, const string &stream_name,
                    const string &since_event_id = "", const string &partition_hint = "");

//! Build URL from components directly (no DB lookup).
string BuildNextUrlDirect(const string &base_url, const string &stream_id, const string &facet,
                          int page_limit, const string &since_event_id = "",
                          const string &partition_hint = "");

} // namespace inferal_relay
} // namespace duckdb
