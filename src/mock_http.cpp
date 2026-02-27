#include "mock_http.hpp"
#include "schema.hpp"
#include "http.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {
namespace inferal_relay {

static void CheckResult(unique_ptr<QueryResult> result, const string &context_msg) {
	if (!result || result->HasError()) {
		string err = result ? result->GetError() : "null result";
		throw InvalidInputException("Mock adapter: %s: %s", context_msg, err);
	}
}

void InstallMockAdapter(ClientContext &context) {
	EnsureSchema(context);

	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	// Create mock_responses table (idempotent)
	CheckResult(con.Query(
	    "CREATE TABLE IF NOT EXISTS inferal_relay.mock_responses ("
	    "  url_pattern VARCHAR NOT NULL, "
	    "  status_code INTEGER DEFAULT 200, "
	    "  body VARCHAR NOT NULL, "
	    "  headers VARCHAR DEFAULT '{}'"
	    ")"), "create table");
	// Clear any leftover rows from a previous install
	con.Query("DELETE FROM inferal_relay.mock_responses");

	DatabaseInstance *db_ptr = &db;

	SetHttpGetter([db_ptr](const string &url, const string & /*api_key*/,
	                        const RetryConfig & /*retry_config*/) -> HttpResponse {
		HttpResponse response;
		try {
			Connection mock_con(*db_ptr);

			auto result = mock_con.Query(
			    "SELECT status_code, body FROM inferal_relay.mock_responses "
			    "WHERE regexp_matches($1::VARCHAR, url_pattern) "
			    "LIMIT 1",
			    url);
			auto &mat = result->Cast<MaterializedQueryResult>();

			if (mat.HasError()) {
				response.status_code = 404;
				response.body = "";
				response.error = "Mock query error: " + mat.GetError();
				return response;
			}
			if (mat.RowCount() == 0) {
				response.status_code = 404;
				response.body = "";
				response.error = "No mock response matched URL: " + url;
				return response;
			}

			response.status_code = mat.GetValue(0, 0).GetValue<int32_t>();
			response.body = mat.GetValue(1, 0).ToString();
			response.error = "";

		} catch (std::exception &e) {
			response.status_code = 0;
			response.body = "";
			response.error = string("Mock adapter error: ") + e.what();
		}
		return response;
	});
}

void ResetMockAdapter() {
	ResetHttpGetter();
}

} // namespace inferal_relay
} // namespace duckdb
