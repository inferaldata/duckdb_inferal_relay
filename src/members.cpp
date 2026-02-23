#include "members.hpp"
#include "schema.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/prepared_statement.hpp"

namespace duckdb {
namespace inferal_relay {

// Helper: cast QueryResult to MaterializedQueryResult
static MaterializedQueryResult &Materialize(unique_ptr<QueryResult> &result) {
	return result->Cast<MaterializedQueryResult>();
}

// Create a Value with JSON logical type from a raw JSON string
static Value JsonValue(const string &json_str) {
	Value v(json_str);
	v.Reinterpret(LogicalType::JSON());
	return v;
}

int64_t StoreMembers(ClientContext &context, const string &stream_name,
                     const vector<ParsedMember> &members, int64_t context_id) {
	if (members.empty()) {
		return 0;
	}

	EnsureSchema(context);

	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	// Count existing members before insert
	auto before = con.Query(
	    "SELECT count(*) FROM inferal_relay.members WHERE stream_name = $1",
	    stream_name);
	auto &before_mat = Materialize(before);
	int64_t count_before = 0;
	if (!before_mat.HasError() && before_mat.RowCount() > 0) {
		count_before = before_mat.GetValue(0, 0).GetValue<int64_t>();
	}

	// Prepare statements for the different insert variants
	auto stmt_full = con.Prepare(
	    "INSERT INTO inferal_relay.members"
	    "(stream_name, event_id, member_id, generated_at, member_path, data, context_id) "
	    "VALUES ($1, $2, $3, $4::TIMESTAMP, $5, $6, $7) "
	    "ON CONFLICT (stream_name, event_id) DO NOTHING");

	auto stmt_no_ts = con.Prepare(
	    "INSERT INTO inferal_relay.members"
	    "(stream_name, event_id, member_id, member_path, data, context_id) "
	    "VALUES ($1, $2, $3, $4, $5, $6) "
	    "ON CONFLICT (stream_name, event_id) DO NOTHING");

	auto stmt_ts_noctx = con.Prepare(
	    "INSERT INTO inferal_relay.members"
	    "(stream_name, event_id, member_id, generated_at, member_path, data) "
	    "VALUES ($1, $2, $3, $4::TIMESTAMP, $5, $6) "
	    "ON CONFLICT (stream_name, event_id) DO NOTHING");

	auto stmt_plain = con.Prepare(
	    "INSERT INTO inferal_relay.members"
	    "(stream_name, event_id, member_id, member_path, data) "
	    "VALUES ($1, $2, $3, $4, $5) "
	    "ON CONFLICT (stream_name, event_id) DO NOTHING");

	for (auto &m : members) {
		auto data_val = JsonValue(m.data);

		if (context_id >= 0 && !m.generated_at.empty()) {
			vector<Value> values = {Value(stream_name), Value(m.event_id), Value(m.member_id),
			                        Value(m.generated_at), Value(m.member_path), data_val,
			                        Value::BIGINT(context_id)};
			stmt_full->Execute(values);
		} else if (context_id >= 0) {
			vector<Value> values = {Value(stream_name), Value(m.event_id), Value(m.member_id),
			                        Value(m.member_path), data_val, Value::BIGINT(context_id)};
			stmt_no_ts->Execute(values);
		} else if (!m.generated_at.empty()) {
			vector<Value> values = {Value(stream_name), Value(m.event_id), Value(m.member_id),
			                        Value(m.generated_at), Value(m.member_path), data_val};
			stmt_ts_noctx->Execute(values);
		} else {
			vector<Value> values = {Value(stream_name), Value(m.event_id), Value(m.member_id),
			                        Value(m.member_path), data_val};
			stmt_plain->Execute(values);
		}
	}

	// Count after insert to determine how many were actually new
	auto after = con.Query(
	    "SELECT count(*) FROM inferal_relay.members WHERE stream_name = $1",
	    stream_name);
	auto &after_mat = Materialize(after);
	int64_t count_after = 0;
	if (!after_mat.HasError() && after_mat.RowCount() > 0) {
		count_after = after_mat.GetValue(0, 0).GetValue<int64_t>();
	}

	return count_after - count_before;
}

} // namespace inferal_relay
} // namespace duckdb
