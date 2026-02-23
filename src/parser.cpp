#include "parser.hpp"
#include "duckdb/common/types/value.hpp"
#include "yyjson.hpp"

namespace duckdb {
namespace inferal_relay {

using namespace duckdb_yyjson; // NOLINT

// Helper: extract a string value from a yyjson value
// Handles both plain strings and {"@value": "...", "@type": "..."} objects
static string ExtractTypedValue(yyjson_val *val) {
	if (!val) {
		return "";
	}
	if (yyjson_is_str(val)) {
		return yyjson_get_str(val);
	}
	if (yyjson_is_obj(val)) {
		yyjson_val *at_value = yyjson_obj_get(val, "@value");
		if (at_value && yyjson_is_str(at_value)) {
			return yyjson_get_str(at_value);
		}
	}
	return "";
}

// Helper: extract partition hint from a URL string like "...?since=X&_p=3s.abcd"
static string ExtractPartitionHint(const string &node_url) {
	auto pos = node_url.find("_p=");
	if (pos == string::npos) {
		return "";
	}
	pos += 3; // skip "_p="
	auto end = node_url.find('&', pos);
	if (end == string::npos) {
		return node_url.substr(pos);
	}
	return node_url.substr(pos, end - pos);
}

ParsedResponse ProcessResponse(const string &response_body) {
	ParsedResponse result;
	result.has_more = false;

	yyjson_doc *doc = yyjson_read(response_body.c_str(), response_body.size(), 0);
	if (!doc) {
		return result;
	}

	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_obj(root)) {
		yyjson_doc_free(doc);
		return result;
	}

	// Extract @context
	yyjson_val *ctx_val = yyjson_obj_get(root, "@context");
	if (ctx_val && !yyjson_is_null(ctx_val) && !yyjson_is_str(ctx_val)) {
		// Serialize context back to JSON string
		yyjson_write_err err;
		char *json_str = yyjson_val_write(ctx_val, 0, nullptr);
		if (json_str) {
			result.context_json = json_str;
			free(json_str);
		}
	}

	// Extract tree:member
	yyjson_val *members_val = yyjson_obj_get(root, "tree:member");
	if (members_val && yyjson_is_arr(members_val)) {
		size_t idx, max;
		yyjson_val *member;
		yyjson_arr_foreach(members_val, idx, max, member) {
			if (!yyjson_is_obj(member)) {
				continue;
			}

			ParsedMember pm;

			// relay:eventId (always a plain string)
			yyjson_val *event_id_val = yyjson_obj_get(member, "relay:eventId");
			if (event_id_val && yyjson_is_str(event_id_val)) {
				pm.event_id = yyjson_get_str(event_id_val);
			}
			if (pm.event_id.empty()) {
				continue; // Skip members without event_id
			}

			// @id
			yyjson_val *id_val = yyjson_obj_get(member, "@id");
			if (id_val && yyjson_is_str(id_val)) {
				pm.member_id = yyjson_get_str(id_val);
			}

			// prov:generatedAtTime
			yyjson_val *gen_at_val = yyjson_obj_get(member, "prov:generatedAtTime");
			pm.generated_at = ExtractTypedValue(gen_at_val);

			// relay:path
			yyjson_val *path_val = yyjson_obj_get(member, "relay:path");
			if (path_val && yyjson_is_str(path_val)) {
				pm.member_path = yyjson_get_str(path_val);
			}

			// Full member as JSON
			char *member_json = yyjson_val_write(member, 0, nullptr);
			if (member_json) {
				pm.data = member_json;
				free(member_json);
			}

			result.members.push_back(std::move(pm));
		}
	}

	// Extract tree:relation
	yyjson_val *relations_val = yyjson_obj_get(root, "tree:relation");
	if (!relations_val || yyjson_is_null(relations_val)) {
		// No relation = caught up
		yyjson_doc_free(doc);
		return result;
	}

	yyjson_val *relation = nullptr;

	if (yyjson_is_obj(relations_val)) {
		// Single object form
		relation = relations_val;
	} else if (yyjson_is_arr(relations_val)) {
		// Find the GreaterThanRelation
		size_t idx, max;
		yyjson_val *rel;
		yyjson_arr_foreach(relations_val, idx, max, rel) {
			yyjson_val *type_val = yyjson_obj_get(rel, "@type");
			if (type_val && yyjson_is_str(type_val)) {
				if (string(yyjson_get_str(type_val)) == "tree:GreaterThanRelation") {
					relation = rel;
					break;
				}
			}
		}
	}

	if (relation) {
		// Extract cursor value from tree:value
		yyjson_val *tree_value = yyjson_obj_get(relation, "tree:value");
		result.next_event_id = ExtractTypedValue(tree_value);

		// Extract partition hint from tree:node URL
		yyjson_val *tree_node = yyjson_obj_get(relation, "tree:node");
		if (tree_node && yyjson_is_str(tree_node)) {
			result.next_partition_hint = ExtractPartitionHint(yyjson_get_str(tree_node));
		}

		result.has_more = true;
	}

	yyjson_doc_free(doc);
	return result;
}

} // namespace inferal_relay
} // namespace duckdb
