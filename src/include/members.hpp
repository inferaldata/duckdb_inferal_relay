#pragma once

#include "duckdb.hpp"
#include "parser.hpp"
#include <vector>

namespace duckdb {
namespace inferal_relay {

//! Store parsed members into inferal_relay.members with ON CONFLICT DO NOTHING dedup.
//! Returns the number of newly inserted rows.
int64_t StoreMembers(ClientContext &context, const string &stream_name,
                     const vector<ParsedMember> &members, int64_t context_id = -1);

} // namespace inferal_relay
} // namespace duckdb
