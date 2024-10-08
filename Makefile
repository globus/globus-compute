.venv-docs:
	python3 -m venv .venv-docs
	.venv-docs/bin/pip install -U pip setuptools
	.venv-docs/bin/pip install './compute_sdk[docs]' './compute_endpoint'

.PHONY: lint docs
lint:
	pre-commit run -a
	cd compute_sdk; tox -e mypy; cd ..
	cd compute_endpoint; tox -e mypy; cd ..

docs: .venv-docs
	# clean the build dir before rebuilding
	cd docs; rm -rf _build/
	cd docs; ../.venv-docs/bin/sphinx-build -W --keep-going -d _build/doctrees -b dirhtml . _build/dirhtml

clean-docs:
	rm -rf .venv-docs/

clean: clean-docs
