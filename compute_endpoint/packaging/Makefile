.DEFAULT: help

PYTHON3 := $(shell which /opt/globus-python/bin/python3 python3 | head -1)

PY_FULL_VERSION := $$($(PYTHON3) -c "import sys; print('{}.{}.{}'.format(*sys.version_info))")
PY_MAJOR_VERSION := $(shell echo $(PY_FULL_VERSION) | cut -d . -f1 )
PY_MINOR_VERSION := $(shell echo $(PY_FULL_VERSION) | cut -d . -f2 )
PY_VERSION := $(PY_MAJOR_VERSION)$(PY_MINOR_VERSION)

VIRTUALENV := venv-$(PY_VERSION)
VENV_PIP := $(VIRTUALENV)/bin/pip
VENV_PY := $(shell pwd)/$(VIRTUALENV)/bin/python

# This if conditional before the PKG_ variables, so as to ensure we have setuptools;
# not so ideal, but at the moment, I don't know how to do better.  Hmm.
ifeq ($(wildcard $(VENV_PY)),)
	_DUMMY := $(shell $(PYTHON3) -mvenv "$(VIRTUALENV)")
	_DUMMY := $(shell $(VENV_PY) -mpip install -U pip setuptools)
endif

PKG_VERSION := $(shell cd ../; $(VENV_PY) setup.py --version | tr '-' '~')
PKG_NAME := $(shell cd ../; $(VENV_PY) setup.py --name | tr '-' '_')
PKG_WHEEL = $(PKG_NAME)-$(PKG_VERSION)-py$(PY_MAJOR_VERSION)-none-any.whl
PREREQS_TARBALL_NAME = $(PKG_NAME)-prereqs-py$(PY_VERSION)-$(PKG_VERSION).tar.xz

OS_CODENAME := $(shell test -f /etc/os-release && . /etc/os-release; echo $${VERSION_CODENAME:-focal})

# with thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
# header text == hash-hash-H
# footer text == hash-hash-F
# targets annotated with hash-hash-dash
.PHONY: help
##H---------------------------------------------------------------------------
##H The available targets are:
##H
##F
##F The main targets of interest are `deb` and `rpm`
help:  ##-(This command) show description of each target
	@sed -ne "/@sed/!s/.*##H//p" $(MAKEFILE_LIST)
	@grep -hE '^[A-Za-z_-]+:.*?##-.*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?##-"}; {printf " \033[36;1m%-30s\033[0m %s\n", $$1, $$2}'
	@sed -ne "/@sed/!s/.*##F//p" $(MAKEFILE_LIST)

.PHONY: show_vars
show_vars:   ##-For debugging, show the Makefile variables; will install a venv
	@echo
	@echo "OS_CODENAME          : $(OS_CODENAME)"
	@echo "PYTHON3              : $(PYTHON3)"
	@echo "PY_FULL_VERSION      : $(PY_FULL_VERSION)"
	@echo "PY_MAJOR_VERSION     : $(PY_MAJOR_VERSION)"
	@echo "PY_MINOR_VERSION     : $(PY_MINOR_VERSION)"
	@echo "PY_VERSION           : $(PY_VERSION)"
	@echo "VIRTUALENV           : $(VIRTUALENV)"
	@echo "VENV_PY              : $(VENV_PY)"
	@echo "VENV_PIP             : $(VENV_PIP)"
	@echo "PKG_NAME             : $(PKG_NAME)"
	@echo "PKG_VERSION          : $(PKG_VERSION)"
	@echo "PREREQS_TARBALL_NAME : $(PREREQS_TARBALL_NAME)"
	@echo "PKG_WHEEL            : $(PKG_WHEEL)"
	@echo
	@echo "  Override python path with PYTHON3 variable:"
	@echo "    $(MAKE) PYTHON3=/path/to/python target(s)"

.PHONY: clean
clean:  ##-Remove the venv, build/ dist/, prereqs tarball, and the package wheel
	rm -rf -- "$(VIRTUALENV)" build/ dist/ "$(PREREQS_TARBALL_NAME)" "$(PKG_WHEEL)"

.PHONY: distclean
distclean: clean  ##-Run `clean` target, then additionally remove venv-*, *tar.xz, *whl
	rm -rf -- venv-* "$(PKG_NAME)"-*tar.xz "$(PKG_NAME)"-*whl

.PHONY: $(VENV_PY)
$(VENV_PY):
	@if [ "$(PY_MAJOR_VERSION)" -ne 3 ] || [ "$(PY_MINOR_VERSION)" -lt 9 ]; then \
		echo "Unsupported python version $(PY_FULL_VERSION). At least 3.9 is required."; \
		echo "To override python path, use the following"; \
		echo "    make PYTHON3=/path/to/python"; \
		exit 1; \
	fi
	$(PYTHON3) -mvenv $(VIRTUALENV)
	. $(VIRTUALENV)/bin/activate
	@$(VENV_PY) -m pip install -U pip setuptools

$(PKG_WHEEL): $(VENV_PY)
	(   rm -rf build/ \
	 && git clone ../../ build/ \
	 && cd build/compute_endpoint/ \
	 && echo -n "    Git Tag: " \
	 && if ! git describe --tags --exact-match; then \
	   { echo "\nBUILD COMPUTE FROM A RELEASE TAG (current branch: $$(git branch --show-current))"; exit 1; } \
	    fi \
	 && rm -rf tests/ \
	 && $(VENV_PY) -m pip install build \
	 && $(VENV_PY) -m build --wheel -o ../../ \
	)

wheel: $(PKG_WHEEL)  ##-Make the wheel (note that this does *not* include dependencies)

$(PREREQS_TARBALL_NAME): $(VENV_PY) $(PKG_WHEEL)
	PYTHON_BIN="$(VENV_PY)" bash create-prereqs-tarball.sh ./build/compute_endpoint/ > "$(PREREQS_TARBALL_NAME)"

prereq_tarball: $(PREREQS_TARBALL_NAME)  ##-Make a tarball of wheel dependencies

dist: $(PREREQS_TARBALL_NAME)  ##-Make the dist/ directory with prereqs and wheel ready for packaging step
	rm -rf dist/ \
	  && mkdir dist/ \
	  && cp $(PREREQS_TARBALL_NAME) $(PKG_WHEEL) dist/

deb_build_needs:  ##-Check that necessary executables are available before starting the build.
	@echo "\033[36;1;40mChecking build dependencies before starting build\033[39;49m ...\n"
	@[ -x "$$(which dpkg-checkbuilddeps)" ] || { echo "Missing 'dpkg-dev' package"; exit 1; }
	@dpkg-checkbuilddeps

deb: deb_build_needs dist  ##-Build a Debian package of the Globus Compute Endpoint (.deb)
	(   cd dist/ \
	 && rm -rf debbuild/ \
	 && mkdir -p debbuild/globus-compute-endpoint/wheels/ \
	 && tar -C debbuild/globus-compute-endpoint/wheels/ --strip 1 -xf "$(PREREQS_TARBALL_NAME)" \
	 && cp -R ../debian debbuild/globus-compute-endpoint/ \
	 && cp $(PKG_WHEEL) debbuild/globus-compute-endpoint/wheels/ \
	 && cd debbuild/globus-compute-endpoint/ \
	 && mv debian/changelog.in.in debian/changelog \
	 && sed -i -e "s/@VERSION@/$(PKG_VERSION)/g" -e "s/@distro@/$(OS_CODENAME)/g" debian/changelog \
	 && dpkg-buildpackage -uc -us \
	)
	@echo "\nDEB package successfully built:"
	@ls -lh dist/debbuild/*deb
