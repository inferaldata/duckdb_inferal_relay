#include "sync.hpp"
#include "schema.hpp"
#include "secrets.hpp"
#include "http.hpp"
#include "url_builder.hpp"
#include "parser.hpp"
#include "context.hpp"
#include "members.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace inferal_relay {

// Helper: cast QueryResult to MaterializedQueryResult
static MaterializedQueryResult &Materialize(unique_ptr<QueryResult> &result) {
	return result->Cast<MaterializedQueryResult>();
}

// ============================================================================
// Helper: ingest a single page (parse + store + advance cursor)
// ============================================================================
struct IngestResult {
	int64_t members_received;
	string next_event_id;
	string next_partition_hint;
	bool has_more;
};

static IngestResult IngestPageInternal(ClientContext &context, const string &stream_name,
                                       const string &response_body) {
	EnsureSchema(context);

	IngestResult result;
	result.members_received = 0;
	result.has_more = false;

	// Parse response
	auto parsed = ProcessResponse(response_body);

	// Upsert context
	int64_t context_id = -1;
	if (!parsed.context_json.empty()) {
		context_id = UpsertContext(context, stream_name, parsed.context_json);
	}

	// Store members
	result.members_received = StoreMembers(context, stream_name, parsed.members, context_id);
	result.next_event_id = parsed.next_event_id;
	result.next_partition_hint = parsed.next_partition_hint;
	result.has_more = parsed.has_more;

	// Load current cursor to get fallback last_event_id
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	auto cursor = con.Query(
	    "SELECT last_event_id FROM inferal_relay.cursors WHERE stream_name = $1",
	    stream_name);
	auto &cursor_mat = Materialize(cursor);
	string current_event_id;
	if (!cursor_mat.HasError() && cursor_mat.RowCount() > 0 && !cursor_mat.GetValue(0, 0).IsNull()) {
		current_event_id = cursor_mat.GetValue(0, 0).ToString();
	}

	// Advance cursor
	string new_event_id = !parsed.next_event_id.empty() ? parsed.next_event_id : current_event_id;
	con.Query(
	    "UPDATE inferal_relay.cursors "
	    "SET last_event_id = $2, partition_hint = $3, "
	    "    last_synced_at = current_timestamp, "
	    "    pages_fetched = pages_fetched + 1, "
	    "    members_received = members_received + $4 "
	    "WHERE stream_name = $1",
	    stream_name, new_event_id, parsed.next_partition_hint,
	    result.members_received);

	return result;
}

// ============================================================================
// Scalar function: inferal_relay_add_stream
// ============================================================================
static void AddStreamFn(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	EnsureSchema(context);

	auto &name_vec = args.data[0];
	auto &base_url_vec = args.data[1];
	auto &stream_id_vec = args.data[2];
	auto &facet_vec = args.data[3];
	auto &page_limit_vec = args.data[4];

	auto &db = DatabaseInstance::GetDatabase(context);

	idx_t count = args.size();
	UnifiedVectorFormat name_data, url_data, sid_data, facet_data, limit_data;
	name_vec.ToUnifiedFormat(count, name_data);
	base_url_vec.ToUnifiedFormat(count, url_data);
	stream_id_vec.ToUnifiedFormat(count, sid_data);
	facet_vec.ToUnifiedFormat(count, facet_data);
	page_limit_vec.ToUnifiedFormat(count, limit_data);

	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto name_idx = name_data.sel->get_index(i);
		auto url_idx = url_data.sel->get_index(i);
		auto sid_idx = sid_data.sel->get_index(i);
		auto facet_idx = facet_data.sel->get_index(i);
		auto limit_idx = limit_data.sel->get_index(i);

		auto name = UnifiedVectorFormat::GetData<string_t>(name_data)[name_idx].GetString();
		auto base_url = UnifiedVectorFormat::GetData<string_t>(url_data)[url_idx].GetString();
		auto stream_id = UnifiedVectorFormat::GetData<string_t>(sid_data)[sid_idx].GetString();
		auto facet = UnifiedVectorFormat::GetData<string_t>(facet_data)[facet_idx].GetString();
		auto page_limit = UnifiedVectorFormat::GetData<int32_t>(limit_data)[limit_idx];

		// Validate inputs before inserting
		if (!StringUtil::StartsWith(base_url, "http://") && !StringUtil::StartsWith(base_url, "https://")) {
			throw InvalidInputException("base_url must start with http:// or https://");
		}
		if (page_limit <= 0 || page_limit > 10000) {
			throw InvalidInputException("page_limit must be between 1 and 10000");
		}

		Connection con(db);
		con.Query(
		    "INSERT INTO inferal_relay.streams(name, base_url, stream_id, facet, page_limit) "
		    "VALUES ($1, $2, $3, $4, $5)",
		    name, base_url, stream_id, facet, page_limit);

		// Create cursor row (no trigger in DuckDB, do it explicitly)
		con.Query(
		    "INSERT INTO inferal_relay.cursors(stream_name) VALUES ($1)",
		    name);

		result_data[i] = StringVector::AddString(result, "Stream '" + name + "' added");
	}
}

// ============================================================================
// Scalar function: inferal_relay_remove_stream
// ============================================================================
static void RemoveStreamFn(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	EnsureSchema(context);

	auto &db = DatabaseInstance::GetDatabase(context);

	auto &name_vec = args.data[0];
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto name = name_vec.GetValue(i).ToString();

		Connection con(db);
		// Delete in reverse FK order
		con.Query("DELETE FROM inferal_relay.context_terms WHERE context_id IN "
		          "(SELECT id FROM inferal_relay.contexts WHERE stream_name = $1)", name);
		con.Query("DELETE FROM inferal_relay.contexts WHERE stream_name = $1", name);
		con.Query("DELETE FROM inferal_relay.members WHERE stream_name = $1", name);
		con.Query("DELETE FROM inferal_relay.sync_log WHERE stream_name = $1", name);
		con.Query("DELETE FROM inferal_relay.cursors WHERE stream_name = $1", name);
		con.Query("DELETE FROM inferal_relay.streams WHERE name = $1", name);

		result_data[i] = StringVector::AddString(result, "Stream '" + name + "' removed");
	}
}

// ============================================================================
// Scalar function: inferal_relay_reset_cursor
// ============================================================================
static void ResetCursorFn(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	EnsureSchema(context);

	auto &db = DatabaseInstance::GetDatabase(context);

	auto &name_vec = args.data[0];
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto name = name_vec.GetValue(i).ToString();

		Connection con(db);
		con.Query(
		    "UPDATE inferal_relay.cursors "
		    "SET last_event_id = NULL, partition_hint = NULL, "
		    "    last_synced_at = NULL, pages_fetched = 0, members_received = 0 "
		    "WHERE stream_name = $1",
		    name);

		result_data[i] = StringVector::AddString(result, "Cursor reset for '" + name + "'");
	}
}

// ============================================================================
// Scalar function: inferal_relay_build_next_url
// ============================================================================
static void BuildNextUrlFn(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	EnsureSchema(context);

	auto &db = DatabaseInstance::GetDatabase(context);
	auto &name_vec = args.data[0];
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto name = name_vec.GetValue(i).ToString();

		// Load cursor state
		Connection con(db);
		auto cursor = con.Query(
		    "SELECT last_event_id, partition_hint FROM inferal_relay.cursors "
		    "WHERE stream_name = $1", name);
		auto &cursor_mat = Materialize(cursor);

		string since, hint;
		if (!cursor_mat.HasError() && cursor_mat.RowCount() > 0) {
			if (!cursor_mat.GetValue(0, 0).IsNull()) {
				since = cursor_mat.GetValue(0, 0).ToString();
			}
			if (!cursor_mat.GetValue(1, 0).IsNull()) {
				hint = cursor_mat.GetValue(1, 0).ToString();
			}
		}

		auto url = BuildNextUrl(context, name, since, hint);
		result_data[i] = StringVector::AddString(result, url);
	}
}

// ============================================================================
// Scalar function: inferal_relay_resolve_term
// ============================================================================
static void ResolveTermFn(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	auto &stream_vec = args.data[0];
	auto &term_vec = args.data[1];
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto stream_name = stream_vec.GetValue(i).ToString();
		auto term = term_vec.GetValue(i).ToString();

		auto resolved = ResolveTerm(context, stream_name, term);
		if (resolved.empty()) {
			validity.SetInvalid(i);
		} else {
			result_data[i] = StringVector::AddString(result, resolved);
		}
	}
}

// ============================================================================
// Scalar function: inferal_relay_version
// ============================================================================
static void VersionFn(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value(EXT_VERSION_INFERAL_RELAY));
}

// ============================================================================
// Table function: inferal_relay_ingest_page
// ============================================================================
struct IngestPageBindData : public FunctionData {
	string stream_name;
	string body;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<IngestPageBindData>();
		copy->stream_name = stream_name;
		copy->body = body;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IngestPageBindData>();
		return stream_name == other.stream_name && body == other.body;
	}
};

struct IngestPageGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> IngestPageBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<IngestPageBindData>();
	bind_data->stream_name = input.inputs[0].ToString();
	bind_data->body = input.inputs[1].ToString();

	names.push_back("members_received");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("next_event_id");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("next_partition_hint");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("has_more");
	return_types.push_back(LogicalType::BOOLEAN);

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> IngestPageInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	return make_uniq<IngestPageGlobalState>();
}

static void IngestPageScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IngestPageBindData>();
	auto &gstate = data.global_state->Cast<IngestPageGlobalState>();

	if (gstate.done) {
		return;
	}
	gstate.done = true;

	auto result = IngestPageInternal(context, bind_data.stream_name, bind_data.body);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(result.members_received));
	output.SetValue(1, 0, result.next_event_id.empty() ? Value(LogicalType::VARCHAR) : Value(result.next_event_id));
	output.SetValue(2, 0, result.next_partition_hint.empty() ? Value(LogicalType::VARCHAR) : Value(result.next_partition_hint));
	output.SetValue(3, 0, Value::BOOLEAN(result.has_more));
}

// ============================================================================
// Table function: inferal_relay_sync
// ============================================================================
struct SyncBindData : public FunctionData {
	string stream_name;
	int32_t max_pages;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<SyncBindData>();
		copy->stream_name = stream_name;
		copy->max_pages = max_pages;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SyncBindData>();
		return stream_name == other.stream_name && max_pages == other.max_pages;
	}
};

struct SyncGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> SyncBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<SyncBindData>();
	bind_data->stream_name = input.inputs[0].ToString();
	bind_data->max_pages = input.inputs.size() > 1 ? input.inputs[1].GetValue<int32_t>() : 100;

	names.push_back("stream");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("pages_fetched");
	return_types.push_back(LogicalType::INTEGER);
	names.push_back("members_received");
	return_types.push_back(LogicalType::INTEGER);
	names.push_back("last_event_id");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("has_more");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("status");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("error");
	return_types.push_back(LogicalType::VARCHAR);

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> SyncInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SyncGlobalState>();
}

static void SyncScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<SyncBindData>();
	auto &gstate = data.global_state->Cast<SyncGlobalState>();

	if (gstate.done) {
		return;
	}
	gstate.done = true;

	EnsureSchema(context);
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	string stream_name = bind_data.stream_name;
	int32_t max_pages = bind_data.max_pages;

	// Load stream config
	auto stream_result = con.Query(
	    "SELECT base_url, stream_id, facet, page_limit, enabled "
	    "FROM inferal_relay.streams WHERE name = $1", stream_name);
	auto &stream_mat = Materialize(stream_result);

	if (stream_mat.HasError() || stream_mat.RowCount() == 0) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value(stream_name));
		output.SetValue(1, 0, Value::INTEGER(0));
		output.SetValue(2, 0, Value::INTEGER(0));
		output.SetValue(3, 0, Value(LogicalType::VARCHAR));
		output.SetValue(4, 0, Value::BOOLEAN(false));
		output.SetValue(5, 0, Value("error"));
		output.SetValue(6, 0, Value("Stream \"" + stream_name + "\" not found"));
		return;
	}

	bool enabled = stream_mat.GetValue(4, 0).GetValue<bool>();
	if (!enabled) {
		output.SetCardinality(1);
		output.SetValue(0, 0, Value(stream_name));
		output.SetValue(1, 0, Value::INTEGER(0));
		output.SetValue(2, 0, Value::INTEGER(0));
		output.SetValue(3, 0, Value(LogicalType::VARCHAR));
		output.SetValue(4, 0, Value::BOOLEAN(false));
		output.SetValue(5, 0, Value("error"));
		output.SetValue(6, 0, Value("Stream \"" + stream_name + "\" is disabled"));
		return;
	}

	auto base_url = stream_mat.GetValue(0, 0).ToString();
	auto stream_id = stream_mat.GetValue(1, 0).ToString();
	auto facet = stream_mat.GetValue(2, 0).ToString();
	auto page_limit = stream_mat.GetValue(3, 0).GetValue<int32_t>();

	// Load cursor
	auto cursor_result = con.Query(
	    "SELECT last_event_id, partition_hint "
	    "FROM inferal_relay.cursors WHERE stream_name = $1", stream_name);
	auto &cursor_mat = Materialize(cursor_result);

	string last_event_id, partition_hint;
	if (!cursor_mat.HasError() && cursor_mat.RowCount() > 0) {
		if (!cursor_mat.GetValue(0, 0).IsNull()) {
			last_event_id = cursor_mat.GetValue(0, 0).ToString();
		}
		if (!cursor_mat.GetValue(1, 0).IsNull()) {
			partition_hint = cursor_mat.GetValue(1, 0).ToString();
		}
	}

	// Look up API key via secrets manager
	string full_url = BuildNextUrlDirect(base_url, stream_id, facet, page_limit);
	string api_key = LookupApiKey(context, stream_name, full_url);

	// Create sync log entry
	con.Query(
	    "INSERT INTO inferal_relay.sync_log(stream_name) VALUES ($1)",
	    stream_name);
	auto log_id_result = con.Query(
	    "SELECT max(id) FROM inferal_relay.sync_log WHERE stream_name = $1",
	    stream_name);
	auto &log_id_mat = Materialize(log_id_result);
	int64_t log_id = log_id_mat.GetValue(0, 0).GetValue<int64_t>();

	int32_t total_pages = 0;
	int32_t total_members = 0;
	bool has_more = true;
	string error_msg;

	try {
		while (has_more && total_pages < max_pages) {
			// Build URL
			auto url = BuildNextUrlDirect(base_url, stream_id, facet, page_limit,
			                               last_event_id, partition_hint);

			// Fetch page
			auto response = HttpGet(url, api_key);

			if (response.status_code != 200) {
				if (response.status_code == 0) {
					throw InvalidInputException("HTTP request failed: %s", response.error);
				}
				throw InvalidInputException("HTTP %d from %s", response.status_code, url);
			}

			// Ingest page
			auto ingest = IngestPageInternal(context, stream_name, response.body);

			total_pages++;
			total_members += ingest.members_received;

			if (!ingest.next_event_id.empty()) {
				last_event_id = ingest.next_event_id;
				partition_hint = ingest.next_partition_hint;
			}

			has_more = ingest.has_more;
		}

		// Update sync log as completed
		con.Query(
		    "UPDATE inferal_relay.sync_log "
		    "SET finished_at = current_timestamp, pages_fetched = $2, "
		    "    members_received = $3, last_event_id = $4, status = 'completed' "
		    "WHERE id = $1",
		    log_id, total_pages, total_members, last_event_id);

	} catch (std::exception &e) {
		error_msg = e.what();
		con.Query(
		    "UPDATE inferal_relay.sync_log "
		    "SET finished_at = current_timestamp, pages_fetched = $2, "
		    "    members_received = $3, last_event_id = $4, status = 'error', "
		    "    error_message = $5 "
		    "WHERE id = $1",
		    log_id, total_pages, total_members, last_event_id, error_msg);
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value(stream_name));
	output.SetValue(1, 0, Value::INTEGER(total_pages));
	output.SetValue(2, 0, Value::INTEGER(total_members));
	output.SetValue(3, 0, last_event_id.empty() ? Value(LogicalType::VARCHAR) : Value(last_event_id));
	output.SetValue(4, 0, Value::BOOLEAN(has_more));
	output.SetValue(5, 0, Value(error_msg.empty() ? "completed" : "error"));
	output.SetValue(6, 0, error_msg.empty() ? Value(LogicalType::VARCHAR) : Value(error_msg));
}

// ============================================================================
// Table function: inferal_relay_status
// ============================================================================
struct StatusBindData : public FunctionData {
	string stream_name;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<StatusBindData>();
		copy->stream_name = stream_name;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		return stream_name == other_p.Cast<StatusBindData>().stream_name;
	}
};

struct StatusGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> StatusBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<StatusBindData>();
	bind_data->stream_name = input.inputs[0].ToString();

	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("base_url");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("stream_id");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("facet");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("page_limit");
	return_types.push_back(LogicalType::INTEGER);
	names.push_back("enabled");
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("last_event_id");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("partition_hint");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("last_synced_at");
	return_types.push_back(LogicalType::TIMESTAMP);
	names.push_back("pages_fetched");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("members_received");
	return_types.push_back(LogicalType::BIGINT);

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> StatusInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<StatusGlobalState>();
}

static void StatusScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<StatusBindData>();
	auto &gstate = data.global_state->Cast<StatusGlobalState>();

	if (gstate.done) {
		return;
	}
	gstate.done = true;

	EnsureSchema(context);
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	auto result = con.Query(
	    "SELECT s.name, s.base_url, s.stream_id, s.facet, s.page_limit, s.enabled, "
	    "       c.last_event_id, c.partition_hint, c.last_synced_at, "
	    "       c.pages_fetched, c.members_received "
	    "FROM inferal_relay.streams s "
	    "JOIN inferal_relay.cursors c ON c.stream_name = s.name "
	    "WHERE s.name = $1", bind_data.stream_name);
	auto &mat = Materialize(result);

	if (mat.HasError() || mat.RowCount() == 0) {
		return; // empty result
	}

	output.SetCardinality(1);
	for (idx_t col = 0; col < 11; col++) {
		output.SetValue(col, 0, mat.GetValue(col, 0));
	}
}

// ============================================================================
// Table function: inferal_relay_stream (stateless)
// ============================================================================
struct LdesReadBindData : public FunctionData {
	string url;
	string api_key;
	int32_t limit;
	string since;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<LdesReadBindData>();
		copy->url = url;
		copy->api_key = api_key;
		copy->limit = limit;
		copy->since = since;
		return std::move(copy);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LdesReadBindData>();
		return url == other.url && api_key == other.api_key &&
		       limit == other.limit && since == other.since;
	}
};

struct LdesReadGlobalState : public GlobalTableFunctionState {
	bool done = false;
	string next_url;
	int32_t pages_fetched = 0;
};

static unique_ptr<FunctionData> LdesReadBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<LdesReadBindData>();
	bind_data->url = input.inputs[0].ToString();

	// Named parameters
	auto it = input.named_parameters.find("api_key");
	if (it != input.named_parameters.end()) {
		bind_data->api_key = it->second.ToString();
	} else {
		// Fall back to secrets manager (match by URL)
		bind_data->api_key = LookupApiKey(context, bind_data->url, bind_data->url);
	}
	it = input.named_parameters.find("max_pages");
	if (it != input.named_parameters.end()) {
		bind_data->limit = it->second.GetValue<int32_t>();
	} else {
		bind_data->limit = 0; // default: all pages
	}
	it = input.named_parameters.find("since");
	if (it != input.named_parameters.end()) {
		bind_data->since = it->second.ToString();
	}

	names.push_back("event_id");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("member_id");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("generated_at");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("member_path");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("data");
	return_types.push_back(LogicalType::JSON());

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> LdesReadInitGlobal(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto state = make_uniq<LdesReadGlobalState>();
	auto &bind_data = input.bind_data->Cast<LdesReadBindData>();

	// Build initial URL with since parameter
	state->next_url = bind_data.url;
	if (!bind_data.since.empty()) {
		// Append since parameter
		state->next_url += (state->next_url.find('?') == string::npos ? "?" : "&");
		state->next_url += "since=" + bind_data.since;
	}

	return std::move(state);
}

static void LdesReadScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LdesReadBindData>();
	auto &gstate = data.global_state->Cast<LdesReadGlobalState>();

	if (gstate.done || gstate.next_url.empty()) {
		return;
	}

	// Fetch one page
	auto response = HttpGet(gstate.next_url, bind_data.api_key);
	if (response.status_code != 200) {
		gstate.done = true;
		return;
	}

	auto parsed = ProcessResponse(response.body);

	idx_t row_count = MinValue<idx_t>(parsed.members.size(), STANDARD_VECTOR_SIZE);
	output.SetCardinality(row_count);

	for (idx_t i = 0; i < row_count; i++) {
		auto &m = parsed.members[i];
		output.SetValue(0, i, Value(m.event_id));
		output.SetValue(1, i, m.member_id.empty() ? Value(LogicalType::VARCHAR) : Value(m.member_id));
		output.SetValue(2, i, m.generated_at.empty() ? Value(LogicalType::VARCHAR) : Value(m.generated_at));
		output.SetValue(3, i, m.member_path.empty() ? Value(LogicalType::VARCHAR) : Value(m.member_path));
		output.SetValue(4, i, Value(m.data));
	}

	gstate.pages_fetched++;

	if (parsed.has_more && (bind_data.limit <= 0 || gstate.pages_fetched < bind_data.limit)) {
		// Build next URL from the base URL + new since parameter
		string base = bind_data.url;
		// Remove existing query params for since
		auto q_pos = base.find('?');
		if (q_pos != string::npos) {
			base = base.substr(0, q_pos);
		}
		gstate.next_url = base;
		gstate.next_url += "?since=" + parsed.next_event_id;
		if (!parsed.next_partition_hint.empty()) {
			gstate.next_url += "&_p=" + parsed.next_partition_hint;
		}
	} else {
		gstate.done = true;
	}
}

// ============================================================================
// Registration
// ============================================================================
void RegisterFunctions(ExtensionLoader &loader) {
	// Scalar functions
	auto add_stream = ScalarFunction(
	    "inferal_relay_add_stream",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::VARCHAR, LogicalType::INTEGER},
	    LogicalType::VARCHAR, AddStreamFn);
	loader.RegisterFunction(add_stream);

	auto remove_stream = ScalarFunction(
	    "inferal_relay_remove_stream",
	    {LogicalType::VARCHAR},
	    LogicalType::VARCHAR, RemoveStreamFn);
	loader.RegisterFunction(remove_stream);

	auto reset_cursor = ScalarFunction(
	    "inferal_relay_reset_cursor",
	    {LogicalType::VARCHAR},
	    LogicalType::VARCHAR, ResetCursorFn);
	loader.RegisterFunction(reset_cursor);

	auto build_url = ScalarFunction(
	    "inferal_relay_build_next_url",
	    {LogicalType::VARCHAR},
	    LogicalType::VARCHAR, BuildNextUrlFn);
	loader.RegisterFunction(build_url);

	auto resolve_term = ScalarFunction(
	    "inferal_relay_resolve_term",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, ResolveTermFn);
	resolve_term.stability = FunctionStability::VOLATILE;
	loader.RegisterFunction(resolve_term);

	auto version = ScalarFunction(
	    "inferal_relay_version",
	    {},
	    LogicalType::VARCHAR, VersionFn);
	loader.RegisterFunction(version);

	// Table functions
	TableFunction ingest_page("inferal_relay_ingest_page",
	                           {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           IngestPageScan, IngestPageBind, IngestPageInitGlobal);
	loader.RegisterFunction(ingest_page);

	TableFunction sync_fn("inferal_relay_sync",
	                        {LogicalType::VARCHAR, LogicalType::INTEGER},
	                        SyncScan, SyncBind, SyncInitGlobal);
	loader.RegisterFunction(sync_fn);

	// Also register a 1-arg variant of sync (default max_pages=100)
	TableFunction sync_fn_1("inferal_relay_sync",
	                          {LogicalType::VARCHAR},
	                          SyncScan, SyncBind, SyncInitGlobal);
	loader.RegisterFunction(sync_fn_1);

	TableFunction status_fn("inferal_relay_status",
	                          {LogicalType::VARCHAR},
	                          StatusScan, StatusBind, StatusInitGlobal);
	loader.RegisterFunction(status_fn);

	TableFunction inferal_relay_stream("inferal_relay_stream",
	                         {LogicalType::VARCHAR},
	                         LdesReadScan, LdesReadBind, LdesReadInitGlobal);
	inferal_relay_stream.named_parameters["api_key"] = LogicalType::VARCHAR;
	inferal_relay_stream.named_parameters["max_pages"] = LogicalType::INTEGER;
	inferal_relay_stream.named_parameters["since"] = LogicalType::VARCHAR;
	loader.RegisterFunction(inferal_relay_stream);
}

} // namespace inferal_relay
} // namespace duckdb
