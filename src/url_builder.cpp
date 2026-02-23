#include "url_builder.hpp"
#include "schema.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {
namespace inferal_relay {

string BuildNextUrlDirect(const string &base_url, const string &stream_id, const string &facet,
                          int page_limit, const string &since_event_id, const string &partition_hint) {
	// Strip trailing slash
	string base = base_url;
	while (!base.empty() && base.back() == '/') {
		base.pop_back();
	}

	// Build path: {base_url}/streams/{stream_id}/{facet}
	string url = base + "/streams/" + stream_id + "/" + facet;

	string sep = "?";

	// Add since parameter if we have a cursor
	if (!since_event_id.empty()) {
		url += sep + "since=" + since_event_id;
		sep = "&";

		// Add partition hint only when since is present
		if (!partition_hint.empty()) {
			url += "&_p=" + partition_hint;
		}
	}

	// Always add limit
	url += sep + "limit=" + to_string(page_limit);

	return url;
}

string BuildNextUrl(ClientContext &context, const string &stream_name,
                    const string &since_event_id, const string &partition_hint) {
	EnsureSchema(context);

	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	auto result = con.Query(
	    "SELECT base_url, stream_id, facet, page_limit "
	    "FROM inferal_relay.streams WHERE name = $1",
	    stream_name);
	auto &mat = result->Cast<MaterializedQueryResult>();

	if (mat.HasError()) {
		throw InvalidInputException("Failed to look up stream '%s': %s", stream_name, mat.GetError());
	}

	if (mat.RowCount() == 0) {
		throw InvalidInputException("Stream \"%s\" not found", stream_name);
	}

	auto base_url = mat.GetValue(0, 0).ToString();
	auto stream_id = mat.GetValue(1, 0).ToString();
	auto facet = mat.GetValue(2, 0).ToString();
	auto page_limit = mat.GetValue(3, 0).GetValue<int32_t>();

	return BuildNextUrlDirect(base_url, stream_id, facet, page_limit, since_event_id, partition_hint);
}

} // namespace inferal_relay
} // namespace duckdb
