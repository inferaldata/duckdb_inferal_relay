#include "context.hpp"
#include "schema.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "yyjson.hpp"

namespace duckdb {
namespace inferal_relay {

using namespace duckdb_yyjson; // NOLINT

// Normalize a JSON-LD context value into a flat object JSON string.
// Handles null, string (remote URL → skip), array (merge inline objects), object.
// Returns empty string if nothing to process.
static string NormalizeContext(const string &context_json) {
	if (context_json.empty()) {
		return "";
	}

	yyjson_doc *doc = yyjson_read(context_json.c_str(), context_json.size(), 0);
	if (!doc) {
		return "";
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root) {
		yyjson_doc_free(doc);
		return "";
	}

	yyjson_mut_doc *mdoc = nullptr;
	yyjson_mut_val *normalized = nullptr;

	if (yyjson_is_null(root) || yyjson_is_str(root)) {
		// String = remote context URL, cannot process
		yyjson_doc_free(doc);
		return "";
	}

	if (yyjson_is_obj(root)) {
		// Already an object, serialize as-is
		char *json = yyjson_val_write(root, 0, nullptr);
		string result = json ? json : "";
		if (json) free(json);
		yyjson_doc_free(doc);
		return result;
	}

	if (yyjson_is_arr(root)) {
		// Merge inline objects, skip strings
		mdoc = yyjson_mut_doc_new(nullptr);
		normalized = yyjson_mut_obj(mdoc);

		size_t idx, max;
		yyjson_val *elem;
		yyjson_arr_foreach(root, idx, max, elem) {
			if (yyjson_is_obj(elem)) {
				size_t k_idx, k_max;
				yyjson_val *key, *val;
				yyjson_obj_foreach(elem, k_idx, k_max, key, val) {
					yyjson_mut_val *mkey = yyjson_mut_strcpy(mdoc, yyjson_get_str(key));
					yyjson_mut_val *mval = yyjson_val_mut_copy(mdoc, val);
					yyjson_mut_obj_add(normalized, mkey, mval);
				}
			}
		}

		// Check if we got anything
		if (yyjson_mut_obj_size(normalized) == 0) {
			yyjson_mut_doc_free(mdoc);
			yyjson_doc_free(doc);
			return "";
		}

		yyjson_mut_doc_set_root(mdoc, normalized);
		char *json = yyjson_mut_write(mdoc, 0, nullptr);
		string result = json ? json : "";
		if (json) free(json);
		yyjson_mut_doc_free(mdoc);
		yyjson_doc_free(doc);
		return result;
	}

	yyjson_doc_free(doc);
	return "";
}

// Compute MD5 hash of a string
static string ComputeMD5(const string &input) {
	MD5Context md5_ctx;
	md5_ctx.Add(input);
	return md5_ctx.FinishHex();
}

// Check if a string looks like a full URI
static bool IsFullUri(const string &s) {
	return StringUtil::StartsWith(s, "http://") ||
	       StringUtil::StartsWith(s, "https://") ||
	       StringUtil::StartsWith(s, "urn:");
}

// Check if a string is a namespace prefix (full URI ending in # or /)
static bool IsPrefix(const string &s) {
	return IsFullUri(s) && (s.back() == '#' || s.back() == '/');
}

// Helper: cast QueryResult to MaterializedQueryResult
static MaterializedQueryResult &Materialize(unique_ptr<QueryResult> &result) {
	return result->Cast<MaterializedQueryResult>();
}

int64_t UpsertContext(Connection &con, ClientContext &context, const string &stream_name, const string &context_json) {
	// Check existing
	string normalized = NormalizeContext(context_json);
	if (normalized.empty()) {
		return -1;
	}

	string hash = ComputeMD5(normalized);

	auto existing = con.Query(
	    "SELECT id FROM inferal_relay.contexts "
	    "WHERE stream_name = $1 AND context_hash = $2",
	    stream_name, hash);
	auto &existing_mat = Materialize(existing);

	if (!existing_mat.HasError() && existing_mat.RowCount() > 0) {
		return existing_mat.GetValue(0, 0).GetValue<int64_t>();
	}

	// Insert new context
	auto insert_result = con.Query(
	    "INSERT INTO inferal_relay.contexts(stream_name, context_hash, context) "
	    "VALUES ($1, $2, $3) RETURNING id",
	    stream_name, hash, normalized);
	auto &insert_mat = Materialize(insert_result);

	if (insert_mat.HasError() || insert_mat.RowCount() == 0) {
		return -1;
	}

	int64_t ctx_id = insert_mat.GetValue(0, 0).GetValue<int64_t>();

	// Parse the normalized context to extract terms
	yyjson_doc *doc = yyjson_read(normalized.c_str(), normalized.size(), 0);
	if (!doc) {
		return ctx_id;
	}
	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		return ctx_id;
	}

	// Collect all keys for two-pass processing
	struct KeyVal {
		string key;
		yyjson_val *val;
	};
	vector<KeyVal> entries;

	size_t k_idx, k_max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(root, k_idx, k_max, key, val) {
		entries.push_back({yyjson_get_str(key), val});
	}

	// Map of known prefixes for expansion
	unordered_map<string, string> prefixes;

	// Pass 1: collect @vocab, @base, and namespace prefixes
	for (auto &entry : entries) {
		if (entry.key == "@vocab" && yyjson_is_str(entry.val)) {
			string iri = yyjson_get_str(entry.val);
			con.Query(
			    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
			    "VALUES ($1, '@vocab', $2, 'vocab')",
			    ctx_id, iri);
			prefixes["@vocab"] = iri;

		} else if (entry.key == "@base" && yyjson_is_str(entry.val)) {
			string iri = yyjson_get_str(entry.val);
			con.Query(
			    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
			    "VALUES ($1, '@base', $2, 'base')",
			    ctx_id, iri);

		} else if (StringUtil::StartsWith(entry.key, "@")) {
			// Skip other JSON-LD keywords
			continue;

		} else if (yyjson_is_str(entry.val)) {
			string val_str = yyjson_get_str(entry.val);
			if (IsPrefix(val_str)) {
				con.Query(
				    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
				    "VALUES ($1, $2, $3, 'prefix')",
				    ctx_id, entry.key, val_str);
				prefixes[entry.key] = val_str;
			}
		}
	}

	// Pass 2: terms (direct IRIs, compact IRIs, expanded definitions)
	for (auto &entry : entries) {
		if (StringUtil::StartsWith(entry.key, "@")) {
			continue;
		}

		if (yyjson_is_str(entry.val)) {
			string val_str = yyjson_get_str(entry.val);

			// Already inserted as prefix in pass 1
			if (IsPrefix(val_str)) {
				continue;
			}

			// Full URI (not ending in # or /) → direct term mapping
			if (IsFullUri(val_str)) {
				con.Query(
				    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
				    "VALUES ($1, $2, $3, 'term')",
				    ctx_id, entry.key, val_str);

			} else if (val_str.find(':') != string::npos) {
				// Compact IRI → expand using known prefixes
				auto colon_pos = val_str.find(':');
				string prefix = val_str.substr(0, colon_pos);
				string suffix = val_str.substr(colon_pos + 1);
				auto it = prefixes.find(prefix);
				if (it != prefixes.end()) {
					con.Query(
					    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
					    "VALUES ($1, $2, $3, 'term')",
					    ctx_id, entry.key, it->second + suffix);
				} else {
					// Prefix not known, store compact IRI as-is
					con.Query(
					    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
					    "VALUES ($1, $2, $3, 'term')",
					    ctx_id, entry.key, val_str);
				}
			} else {
				// Other string value → term
				con.Query(
				    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
				    "VALUES ($1, $2, $3, 'term')",
				    ctx_id, entry.key, val_str);
			}

		} else if (yyjson_is_obj(entry.val)) {
			// Expanded term definition: {"@id": "...", "@type": "..."}
			yyjson_val *id_val = yyjson_obj_get(entry.val, "@id");
			if (id_val && yyjson_is_str(id_val)) {
				string id_str = yyjson_get_str(id_val);

				// Expand compact IRI in @id if possible
				if (id_str.find(':') != string::npos && !IsFullUri(id_str)) {
					auto colon_pos = id_str.find(':');
					string prefix = id_str.substr(0, colon_pos);
					string suffix = id_str.substr(colon_pos + 1);
					auto it = prefixes.find(prefix);
					if (it != prefixes.end()) {
						id_str = it->second + suffix;
					}
				}

				con.Query(
				    "INSERT INTO inferal_relay.context_terms(context_id, term, iri, term_type) "
				    "VALUES ($1, $2, $3, 'term')",
				    ctx_id, entry.key, id_str);
			}
		}
	}

	yyjson_doc_free(doc);
	return ctx_id;
}

int64_t UpsertContext(ClientContext &context, const string &stream_name, const string &context_json) {
	EnsureSchema(context);
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);
	return UpsertContext(con, context, stream_name, context_json);
}

string ResolveTerm(ClientContext &context, const string &stream_name, const string &term, int64_t context_id) {
	EnsureSchema(context);

	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	// Determine context to use
	int64_t ctx_id = context_id;
	if (ctx_id < 0) {
		auto ctx_result = con.Query(
		    "SELECT id FROM inferal_relay.contexts "
		    "WHERE stream_name = $1 ORDER BY first_seen DESC LIMIT 1",
		    stream_name);
		auto &ctx_mat = Materialize(ctx_result);
		if (ctx_mat.HasError() || ctx_mat.RowCount() == 0) {
			return "";
		}
		ctx_id = ctx_mat.GetValue(0, 0).GetValue<int64_t>();
	}

	// 1. Exact match
	auto exact = con.Query(
	    "SELECT iri FROM inferal_relay.context_terms "
	    "WHERE context_id = $1 AND term = $2",
	    ctx_id, term);
	auto &exact_mat = Materialize(exact);
	if (!exact_mat.HasError() && exact_mat.RowCount() > 0) {
		return exact_mat.GetValue(0, 0).ToString();
	}

	// 2. Prefix expansion: split on first colon
	auto colon_pos = term.find(':');
	if (colon_pos != string::npos) {
		string prefix = term.substr(0, colon_pos);
		string suffix = term.substr(colon_pos + 1);

		auto prefix_result = con.Query(
		    "SELECT iri FROM inferal_relay.context_terms "
		    "WHERE context_id = $1 AND term = $2 AND term_type = 'prefix'",
		    ctx_id, prefix);
		auto &prefix_mat = Materialize(prefix_result);
		if (!prefix_mat.HasError() && prefix_mat.RowCount() > 0) {
			return prefix_mat.GetValue(0, 0).ToString() + suffix;
		}
	}

	// 3. @vocab fallback
	auto vocab = con.Query(
	    "SELECT iri FROM inferal_relay.context_terms "
	    "WHERE context_id = $1 AND term = '@vocab'",
	    ctx_id);
	auto &vocab_mat = Materialize(vocab);
	if (!vocab_mat.HasError() && vocab_mat.RowCount() > 0) {
		return vocab_mat.GetValue(0, 0).ToString() + term;
	}

	return "";
}

} // namespace inferal_relay
} // namespace duckdb
