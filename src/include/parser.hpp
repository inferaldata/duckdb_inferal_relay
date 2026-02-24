#pragma once

#include "duckdb.hpp"
#include <vector>

namespace duckdb {
namespace inferal_relay {

struct ParsedMember {
	string event_id;
	string member_id;
	string generated_at; // ISO 8601 timestamp string
	string member_path;
	string data; // raw JSON string
};

struct ParsedResponse {
	vector<ParsedMember> members;
	string next_event_id;
	string next_partition_hint;
	bool has_more;
	string context_json; // raw JSON string of @context, empty if null/string

	// Progress hints from relay:progress (optional, -1 = absent)
	int64_t remaining_items = -1;
	bool sealed = false;
	int32_t remaining_partitions = 0;
};

//! Parse an LDES JSON-LD response body.
ParsedResponse ProcessResponse(const string &response_body);

} // namespace inferal_relay
} // namespace duckdb
