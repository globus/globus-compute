
# Friendly reminder before running 'local'* smoke tests:
# $ export COMPUTE_LOCAL_ENDPOINT_ID=<id_of_your_local_endpoint>
# and optionally:
# $ export COMPUTE_LOCAL_KNOWN_FUNCTION_ID=<id_of_function_in_your_local_setup>

.PHONY: prod dev local
prod:
	tox
dev:
	tox -e localdeps -- --compute-config dev
local_with_dev_sdk:
	@if [ -z "${COMPUTE_LOCAL_ENDPOINT_ID}" ]; then echo "Missing exported COMPUTE_LOCAL_ENDPOINT_ID"; exit 1; fi
	tox -e localdeps -- --compute-config local
local_with_published_sdk:
	@if [ -z "${COMPUTE_LOCAL_ENDPOINT_ID}" ]; then echo "Missing exported COMPUTE_LOCAL_ENDPOINT_ID"; exit 1; fi
	tox -- --compute-config local
