#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {
namespace inferal_relay {

struct HttpResponse {
	int status_code;
	string body;
	string error;
};

//! Perform an HTTP GET request using duckdb_httplib.
HttpResponse HttpGet(const string &url, const string &api_key = "");

} // namespace inferal_relay
} // namespace duckdb
