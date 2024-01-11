
# Friendly reminder before running 'local'* smoke tests:
# $ export COMPUTE_LOCAL_ENDPOINT_ID=<id_of_your_local_endpoint>
# and optionally:
# $ export COMPUTE_LOCAL_KNOWN_FUNCTION_ID=<id_of_function_in_your_local_setup>

.PHONY: prod staging test sandbox preview integration local
prod:
	tox
staging:
	tox -e localdeps -- --compute-config staging
test:
	tox -e localdeps -- --compute-config test
sandbox:
	tox -e localdeps -- --compute-config sandbox
preview:
	tox -e localdeps -- --compute-config preview
integration:
	tox -e localdeps -- --compute-config integration
local_with_dev_sdk:
	@if [ -z "${COMPUTE_LOCAL_ENDPOINT_ID}" ]; then echo "Missing exported COMPUTE_LOCAL_ENDPOINT_ID"; exit 1; fi
	tox -e localdeps -- --compute-config local
local_with_published_sdk:
	@if [ -z "${COMPUTE_LOCAL_ENDPOINT_ID}" ]; then echo "Missing exported COMPUTE_LOCAL_ENDPOINT_ID"; exit 1; fi
	tox -- --compute-config local
