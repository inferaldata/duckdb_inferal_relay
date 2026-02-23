#include "secrets.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

namespace duckdb {
namespace inferal_relay {

static unique_ptr<BaseSecret> CreateInferalRelaySecret(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	secret->redact_keys = {"api_key"};

	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);
		if (lower_name == "api_key") {
			secret->secret_map["api_key"] = named_param.second;
		} else {
			throw InvalidInputException("Unknown parameter for inferal_relay secret: '%s'", lower_name);
		}
	}

	return std::move(secret);
}

void RegisterSecretType(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = "inferal_relay";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	secret_type.extension = "inferal_relay";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction create_func = {"inferal_relay", "config", CreateInferalRelaySecret};
	create_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	loader.RegisterFunction(create_func);
}

string LookupApiKey(ClientContext &context, const string &stream_name, const string &stream_url) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	// Try to find a secret matching the stream name first, then URL
	// DuckDB's secret manager matches by scope longest-prefix
	// We try the stream name as a path, then fall back to the URL
	auto secret_match = secret_manager.LookupSecret(transaction, stream_name, "inferal_relay");
	if (!secret_match.HasMatch()) {
		// Try URL-based match
		secret_match = secret_manager.LookupSecret(transaction, stream_url, "inferal_relay");
	}
	if (!secret_match.HasMatch()) {
		// Try unscoped (empty path)
		secret_match = secret_manager.LookupSecret(transaction, "", "inferal_relay");
	}

	if (!secret_match.HasMatch()) {
		return "";
	}

	auto &secret = secret_match.GetSecret();
	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret);
	Value api_key_val;
	if (kv_secret.TryGetValue("api_key", api_key_val)) {
		return api_key_val.ToString();
	}
	return "";
}

} // namespace inferal_relay
} // namespace duckdb
