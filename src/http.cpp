#include "http.hpp"
#include "duckdb/common/http_util.hpp"
#include "httplib.hpp"

// httplib namespace depends on whether OpenSSL is enabled
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
namespace httplib = duckdb_httplib_openssl;
#else
namespace httplib = duckdb_httplib;
#endif

namespace duckdb {
namespace inferal_relay {

HttpResponse HttpGet(const string &url, const string &api_key) {
	HttpResponse result;
	result.status_code = 0;

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
		} else {
			result.status_code = 0;
			result.error = "HTTP request failed: " + to_string(res.error());
		}
	} catch (std::exception &e) {
		result.status_code = 0;
		result.error = string("HTTP error: ") + e.what();
	}

	return result;
}

} // namespace inferal_relay
} // namespace duckdb
