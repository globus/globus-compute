.venv-docs:
	python -m venv .venv-docs
	.venv-docs/bin/pip install -U pip setuptools
	.venv-docs/bin/pip install './funcx_sdk[docs]' './funcx_endpoint'

.PHONY: docs
docs: .venv-docs
	# clean the build dir before rebuilding
	cd docs; rm -rf _build/
	cd docs; ../.venv-docs/bin/sphinx-build -d _build/doctrees -b dirhtml . _build/dirhtml

clean:
	rm -rf .venv-docs/
