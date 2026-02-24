#define DUCKDB_EXTENSION_MAIN

#include "inferal_relay_extension.hpp"
#include "schema.hpp"
#include "secrets.hpp"
#include "sync.hpp"
#include "duckdb.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register our custom secret type for API key management
	inferal_relay::RegisterSecretType(loader);

	// Register all scalar and table functions
	inferal_relay::RegisterFunctions(loader);
}

void InferalRelayExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string InferalRelayExtension::Name() {
	return "inferal_relay";
}

std::string InferalRelayExtension::Version() const {
	return EXT_VERSION_INFERAL_RELAY;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(inferal_relay, loader) {
	duckdb::LoadInternal(loader);
}
}
