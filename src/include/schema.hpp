#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace inferal_relay {

//! Ensure the inferal_relay schema and all tables exist.
//! Safe to call multiple times (idempotent).
void EnsureSchema(ClientContext &context);

} // namespace inferal_relay
} // namespace duckdb
