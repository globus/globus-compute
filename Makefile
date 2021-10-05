.venv-docs:
	python -m venv .venv-docs
	.venv-docs/bin/pip install -U pip setuptools
	.venv-docs/bin/pip install ./funcx_sdk ./funcx_endpoint
	.venv-docs/bin/pip install 'sphinx<5' 'furo==2021.09.08' 'nbsphinx==0.8.7'

.PHONY: lint docs
lint:
	pre-commit run -a
docs: .venv-docs
	# clean the build dir before rebuilding
	cd docs; rm -rf _build/
	cd docs; ../.venv-docs/bin/sphinx-build -d _build/doctrees -b dirhtml . _build/dirhtml


clean:
	rm -rf .venv-docs/
