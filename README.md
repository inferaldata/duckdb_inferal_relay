# duckdb_inferal_relay

DuckDB extension for consuming [LDES](https://w3id.org/ldes/specification) streams from [Inferal Relay](https://relay.inferal.com).

## Build

```bash
make release      # Build extension
make duckdb       # Build + launch DuckDB CLI with extension loaded
make duckdb DB=dev.duckdb  # With persistent database
make test         # Run tests
```

Requires: cmake, C++ compiler. No Docker needed.

## Quick start

```sql
-- Add a stream
SELECT inferal_relay_add_stream('my-stream', 'https://relay.inferal.com', 'acme', 'snapshots', 100);

-- Set up credentials via DuckDB Secrets Manager
CREATE PERSISTENT SECRET (
    TYPE inferal_relay,
    API_KEY 'sk-your-key',
    SCOPE 'my-stream'
);

-- Sync data (fetches pages until caught up, max 100 pages)
SELECT * FROM inferal_relay_sync('my-stream');

-- Or with a custom page limit
SELECT * FROM inferal_relay_sync('my-stream', 10);

-- Query members
SELECT member_id, generated_at, data
FROM inferal_relay.members
WHERE stream_name = 'my-stream'
ORDER BY generated_at DESC
LIMIT 10;

-- Or sync + query in one call (auto-refreshing table)
SELECT * FROM inferal_relay_live('my-stream');
COPY (SELECT * FROM inferal_relay_live('my-stream')) TO 'data.parquet';
```

## External sync (no outbound HTTP)

For environments where the extension cannot make HTTP requests:

```sql
-- Get the URL to fetch externally
SELECT inferal_relay_build_next_url('my-stream');

-- Fetch with curl, a worker, etc., then push the body:
SELECT * FROM inferal_relay_ingest_page('my-stream', $body);
-- Returns: members_received, next_event_id, next_partition_hint, has_more

-- Repeat while has_more = true
```

## Stateless table function

Read LDES pages directly as a table (no persistent state):

```sql
-- Read entire stream as rows (follows pagination automatically)
SELECT * FROM inferal_relay_stream('https://relay.inferal.com/streams/acme/snapshots?limit=100');

-- API key is resolved from secrets manager (matched by URL prefix)
-- or passed explicitly:
SELECT * FROM inferal_relay_stream(
    'https://relay.inferal.com/streams/acme/snapshots?limit=100',
    api_key := 'sk-your-key'
);

-- Cap to N pages
SELECT * FROM inferal_relay_stream(
    'https://relay.inferal.com/streams/acme/snapshots?limit=100',
    max_pages := 5
);

-- Resume from a specific event ID
SELECT * FROM inferal_relay_stream(
    'https://relay.inferal.com/streams/acme/snapshots?limit=100',
    since := '018f0001-0000-7000-8000-000000000000'
);

-- Materialize into a table
CREATE TABLE my_data AS
SELECT * FROM inferal_relay_stream('https://relay.inferal.com/streams/acme/snapshots?limit=100');
```

## Live table function

Combines sync and query into a single call. Fetches new pages from the remote, then returns all stored members:

```sql
-- Sync + query in one call
SELECT * FROM inferal_relay_live('my-stream');

-- With explicit page limit
SELECT * FROM inferal_relay_live('my-stream', 50);

-- Export to Parquet
COPY (SELECT * FROM inferal_relay_live('my-stream')) TO 'data.parquet';
```

Returns columns: `event_id VARCHAR, member_id VARCHAR, generated_at TIMESTAMP, member_path VARCHAR, data JSON`.

If sync fails due to a transient HTTP error, the function still returns existing members from the database rather than throwing an error. Stream-not-found and disabled-stream errors throw immediately since they indicate configuration problems.

## Credentials

Uses DuckDB's built-in Secrets Manager:

```sql
-- Per-stream secret (exact match on stream name)
CREATE PERSISTENT SECRET (
    TYPE inferal_relay,
    API_KEY 'sk-acme-xxx',
    SCOPE 'acme'
);

-- Per-host secret (longest prefix match on URL)
-- Also used by inferal_relay_stream() automatically
CREATE PERSISTENT SECRET (
    TYPE inferal_relay,
    API_KEY 'sk-global-yyy',
    SCOPE 'https://relay.inferal.com'
);

-- Global default (no scope, fallback)
CREATE PERSISTENT SECRET (
    TYPE inferal_relay,
    API_KEY 'sk-default-zzz'
);

-- Session-only secret
CREATE SECRET (
    TYPE inferal_relay,
    API_KEY 'sk-temp',
    SCOPE 'test-stream'
);

-- List secrets (values hidden)
SELECT * FROM duckdb_secrets();
```

## Resolve compact IRIs

```sql
SELECT inferal_relay_resolve_term('my-stream', 'relay:eventId');
-- → 'https://relay.inferal.com/ontology#eventId'

SELECT inferal_relay_resolve_term('my-stream', 'name');
-- → 'https://schema.org/name' (falls back to @vocab)
```

## Functions

### Scalar functions

| Function | Description |
|----------|-------------|
| `inferal_relay_add_stream(name, base_url, stream_id, facet, page_limit)` | Add a stream + cursor |
| `inferal_relay_remove_stream(name)` | Remove stream and all data |
| `inferal_relay_reset_cursor(name)` | Reset cursor for re-sync |
| `inferal_relay_build_next_url(name)` | Get next fetch URL |
| `inferal_relay_resolve_term(stream, term)` | Expand compact IRI |
| `inferal_relay_version()` | Extension version |

### Table functions

| Function | Description |
|----------|-------------|
| `inferal_relay_sync(stream [, max_pages])` | Full sync loop with HTTP |
| `inferal_relay_ingest_page(stream, body)` | Ingest a pre-fetched page |
| `inferal_relay_status(stream)` | Stream + cursor status |
| `inferal_relay_live(stream [, max_pages])` | Sync + query: fetches new pages, returns all members |
| `inferal_relay_stream(url [, api_key, max_pages, since])` | Stateless LDES reader (follows pagination, resolves secrets by URL) |

## Tables

All tables are in the `inferal_relay` schema (created on first use):

| Table | Purpose |
|-------|---------|
| `streams` | Stream endpoint configuration |
| `cursors` | Pagination state (1:1 with streams) |
| `members` | Stored LDES members with dedup |
| `contexts` | Deduplicated JSON-LD contexts |
| `context_terms` | Parsed term-to-IRI mappings |
| `sync_log` | Audit trail of sync operations |

## License

Apache License 2.0
