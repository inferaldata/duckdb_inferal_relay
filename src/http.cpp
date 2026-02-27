#include "http.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/printer.hpp"
#include "httplib.hpp"

#include <chrono>
#include <mutex>
#include <random>
#include <thread>

// httplib namespace depends on whether OpenSSL is enabled
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
namespace httplib = duckdb_httplib_openssl;
#else
namespace httplib = duckdb_httplib;
#endif

namespace duckdb {
namespace inferal_relay {

static std::mutex g_http_getter_mutex;
static HttpGetFn g_custom_http_getter;

void SetHttpGetter(HttpGetFn fn) {
	std::lock_guard<std::mutex> lock(g_http_getter_mutex);
	g_custom_http_getter = std::move(fn);
}

void ResetHttpGetter() {
	std::lock_guard<std::mutex> lock(g_http_getter_mutex);
	g_custom_http_getter = nullptr;
}

static bool IsRetryable(int status_code) {
	if (status_code == 0) {
		return true; // connection error
	}
	if (status_code == 429) {
		return true; // rate limited
	}
	if (status_code >= 500 && status_code < 600) {
		return true; // server error
	}
	return false;
}

static int ComputeDelayMs(int initial_delay_ms, int attempt, int max_delay_ms, double jitter_factor) {
	// Exponential backoff: initial_delay_ms * 2^attempt
	int base_delay = initial_delay_ms;
	for (int i = 0; i < attempt; i++) {
		base_delay *= 2;
		if (base_delay > max_delay_ms) {
			base_delay = max_delay_ms;
			break;
		}
	}
	if (base_delay > max_delay_ms) {
		base_delay = max_delay_ms;
	}

	// Apply jitter: uniform random in [base * (1 - jitter), base * (1 + jitter)]
	thread_local std::mt19937 rng(std::random_device{}());
	double lo = base_delay * (1.0 - jitter_factor);
	double hi = base_delay * (1.0 + jitter_factor);
	std::uniform_real_distribution<double> dist(lo, hi);
	return static_cast<int>(dist(rng));
}

static int ParseRetryAfterMs(const httplib::Result &res) {
	if (!res || !res->has_header("Retry-After")) {
		return 0;
	}
	auto value = res->get_header_value("Retry-After");
	try {
		int seconds = std::stoi(value);
		if (seconds < 0) {
			return 0;
		}
		// Cap at 300 seconds
		if (seconds > 300) {
			seconds = 300;
		}
		return seconds * 1000;
	} catch (...) {
		return 0;
	}
}

HttpResponse HttpGet(const string &url, const string &api_key, const RetryConfig &retry_config) {
	{
		std::lock_guard<std::mutex> lock(g_http_getter_mutex);
		if (g_custom_http_getter) {
			return g_custom_http_getter(url, api_key, retry_config);
		}
	}

	HttpResponse result;
	result.status_code = 0;

	int max_attempts = 1 + retry_config.max_retries;

	for (int attempt = 0; attempt < max_attempts; attempt++) {
		try {
			// Decompose URL into proto_host_port and path
			string path, proto_host_port;
			HTTPUtil::DecomposeURL(url, path, proto_host_port);

			httplib::Client client(proto_host_port);
			client.set_read_timeout(30);
			client.set_connection_timeout(10);
			client.set_follow_location(true);

			httplib::Headers headers;
			headers.insert({"Accept", "application/ld+json"});
			headers.insert({"User-Agent", "duckdb-inferal-relay/0.1.0"});

			if (!api_key.empty()) {
				headers.insert({"Authorization", "Bearer " + api_key});
			}

			auto res = client.Get(path, headers);
			if (res) {
				result.status_code = res->status;
				result.body = res->body;
				result.error = "";
			} else {
				result.status_code = 0;
				result.error = "HTTP request failed: " + to_string(res.error());
			}

			// Check if we should retry
			if (!IsRetryable(result.status_code) || attempt == max_attempts - 1) {
				return result;
			}

			// Determine delay
			int delay_ms;
			if (result.status_code == 429) {
				int retry_after_ms = ParseRetryAfterMs(res);
				if (retry_after_ms > 0) {
					delay_ms = retry_after_ms;
				} else {
					delay_ms = ComputeDelayMs(retry_config.initial_delay_ms, attempt,
					                          retry_config.max_delay_ms, retry_config.jitter_factor);
				}
			} else {
				delay_ms = ComputeDelayMs(retry_config.initial_delay_ms, attempt,
				                          retry_config.max_delay_ms, retry_config.jitter_factor);
			}

			Printer::PrintF("HTTP retry %d/%d for %s (status %d), waiting %dms\n",
			                attempt + 1, retry_config.max_retries, url, result.status_code, delay_ms);

			std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

		} catch (std::exception &e) {
			result.status_code = 0;
			result.error = string("HTTP error: ") + e.what();

			if (attempt == max_attempts - 1) {
				return result;
			}

			int delay_ms = ComputeDelayMs(retry_config.initial_delay_ms, attempt,
			                              retry_config.max_delay_ms, retry_config.jitter_factor);

			Printer::PrintF("HTTP retry %d/%d for %s (exception: %s), waiting %dms\n",
			                attempt + 1, retry_config.max_retries, url, e.what(), delay_ms);

			std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
		}
	}

	return result;
}

} // namespace inferal_relay
} // namespace duckdb
