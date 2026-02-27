#include "schema.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {
namespace inferal_relay {

void EnsureSchema(ClientContext &context) {
	// Use a connection from the same database
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	// Quick check: if the schema and a key table already exist, skip DDL
	auto probe = con.Query(
	    "SELECT 1 FROM information_schema.tables "
	    "WHERE table_schema = 'inferal_relay' AND table_name = 'streams' LIMIT 1");
	if (probe && !probe->HasError()) {
		auto &mat = probe->Cast<MaterializedQueryResult>();
		if (mat.RowCount() > 0) {
			return;
		}
	}

	// Create schema
	con.Query("CREATE SCHEMA IF NOT EXISTS inferal_relay");

	// Create sequences for auto-increment IDs
	con.Query("CREATE SEQUENCE IF NOT EXISTS inferal_relay.contexts_id_seq");
	con.Query("CREATE SEQUENCE IF NOT EXISTS inferal_relay.members_id_seq");
	con.Query("CREATE SEQUENCE IF NOT EXISTS inferal_relay.sync_log_id_seq");

	// Streams table
	con.Query(R"(
		CREATE TABLE IF NOT EXISTS inferal_relay.streams (
			name        VARCHAR PRIMARY KEY,
			base_url    VARCHAR NOT NULL,
			stream_id   VARCHAR NOT NULL,
			facet       VARCHAR NOT NULL DEFAULT 'snapshots',
			page_limit  INTEGER NOT NULL DEFAULT 100,
			enabled     BOOLEAN NOT NULL DEFAULT true,
			created_at  TIMESTAMP NOT NULL DEFAULT current_timestamp,
			updated_at  TIMESTAMP NOT NULL DEFAULT current_timestamp,
			CHECK (page_limit > 0 AND page_limit <= 10000),
			CHECK (base_url LIKE 'http://%' OR base_url LIKE 'https://%')
		)
	)");

	// Cursors table (1:1 with streams)
	con.Query(R"(
		CREATE TABLE IF NOT EXISTS inferal_relay.cursors (
			stream_name      VARCHAR PRIMARY KEY,
			last_event_id    VARCHAR,
			partition_hint   VARCHAR,
			last_synced_at   TIMESTAMP,
			pages_fetched    BIGINT NOT NULL DEFAULT 0,
			members_received BIGINT NOT NULL DEFAULT 0
		)
	)");

	// Contexts table (JSON-LD contexts deduplicated by hash)
	con.Query(R"(
		CREATE TABLE IF NOT EXISTS inferal_relay.contexts (
			id           BIGINT PRIMARY KEY DEFAULT nextval('inferal_relay.contexts_id_seq'),
			stream_name  VARCHAR NOT NULL,
			context_hash VARCHAR NOT NULL,
			context      JSON NOT NULL,
			first_seen   TIMESTAMP NOT NULL DEFAULT current_timestamp,
			UNIQUE (stream_name, context_hash)
		)
	)");

	// Context terms table
	con.Query(R"(
		CREATE TABLE IF NOT EXISTS inferal_relay.context_terms (
			context_id BIGINT NOT NULL,
			term       VARCHAR NOT NULL,
			iri        VARCHAR NOT NULL,
			term_type  VARCHAR NOT NULL,
			PRIMARY KEY (context_id, term)
		)
	)");

	// Members table
	con.Query(R"(
		CREATE TABLE IF NOT EXISTS inferal_relay.members (
			id           BIGINT PRIMARY KEY DEFAULT nextval('inferal_relay.members_id_seq'),
			stream_name  VARCHAR NOT NULL,
			event_id     VARCHAR NOT NULL,
			member_id    VARCHAR,
			generated_at TIMESTAMP,
			member_path  VARCHAR,
			data         JSON NOT NULL,
			fetched_at   TIMESTAMP NOT NULL DEFAULT current_timestamp,
			context_id   BIGINT,
			UNIQUE (stream_name, event_id)
		)
	)");

	// Sync log table
	con.Query(R"(
		CREATE TABLE IF NOT EXISTS inferal_relay.sync_log (
			id               BIGINT PRIMARY KEY DEFAULT nextval('inferal_relay.sync_log_id_seq'),
			stream_name      VARCHAR NOT NULL,
			started_at       TIMESTAMP NOT NULL DEFAULT current_timestamp,
			finished_at      TIMESTAMP,
			pages_fetched    INTEGER NOT NULL DEFAULT 0,
			members_received INTEGER NOT NULL DEFAULT 0,
			last_event_id    VARCHAR,
			status           VARCHAR NOT NULL DEFAULT 'running',
			error_message    VARCHAR
		)
	)");

}

} // namespace inferal_relay
} // namespace duckdb
