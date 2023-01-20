
# Friendly reminder before running 'local'* smoke tests:
# $ export FUNCX_LOCAL_ENDPOINT_ID=<id_of_your_local_endpoint>
# and optionally:
# $ export FUNCX_LOCAL_KNOWN_FUNCTION_ID=<id_of_function_in_your_local_setup>

.PHONY: prod dev local
prod:
	tox
dev:
	tox -e localdeps -- --funcx-config dev
local_with_dev_sdk:
	@if [ -z "${FUNCX_LOCAL_ENDPOINT_ID}" ]; then echo "Missing exported FUNCX_LOCAL_ENDPOINT_ID"; exit 1; fi
	tox -e localdeps -- --funcx-config local
local_with_published_sdk:
	@if [ -z "${FUNCX_LOCAL_ENDPOINT_ID}" ]; then echo "Missing exported FUNCX_LOCAL_ENDPOINT_ID"; exit 1; fi
	tox -- --funcx-config local
