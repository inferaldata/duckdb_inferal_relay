#pragma once

#include "duckdb.hpp"
#include <functional>
#include <string>

namespace duckdb {
namespace inferal_relay {

struct HttpResponse {
	int status_code;
	string body;
	string error;
};

struct RetryConfig {
	int max_retries = 3;
	int initial_delay_ms = 500;
	int max_delay_ms = 30000;
	double jitter_factor = 0.25;
};

//! Perform an HTTP GET request using duckdb_httplib.
//! Retries on transient failures (connection errors, 5xx, 429) with
//! exponential backoff and jitter.
HttpResponse HttpGet(const string &url, const string &api_key = "",
                     const RetryConfig &retry_config = RetryConfig{});

//! Function type for pluggable HTTP GET implementations.
using HttpGetFn = std::function<HttpResponse(const string &, const string &, const RetryConfig &)>;

//! Replace the default HTTP getter with a custom implementation.
void SetHttpGetter(HttpGetFn fn);

//! Restore the default (httplib-based) HTTP getter.
void ResetHttpGetter();

} // namespace inferal_relay
} // namespace duckdb
