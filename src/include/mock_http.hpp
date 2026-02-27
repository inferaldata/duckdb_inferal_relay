#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace inferal_relay {

//! Create the mock_responses table and switch HttpGet to a table-driven mock.
void InstallMockAdapter(ClientContext &context);

//! Restore the real (httplib-based) HTTP getter.
void ResetMockAdapter();

} // namespace inferal_relay
} // namespace duckdb
