.DEFAULT: help

# Work with the (apparently) different setup in OpenSUSE, where `make` doesn't
# invoke /bin/sh in the same manner as other platforms.
PYTHON3 := $$($(SHELL) -c "command -v /opt/globus-python/bin/python3")

PY_FULL_VERSION := $$($(PYTHON3) -c "import sys; print('{}.{}.{}'.format(*sys.version_info))")
PY_MAJOR_VERSION := $(shell echo -n $(PY_FULL_VERSION) | cut -d . -f1 )
PY_MINOR_VERSION := $(shell echo -n $(PY_FULL_VERSION) | cut -d . -f2 )
PY_VERSION := $(PY_MAJOR_VERSION)$(PY_MINOR_VERSION)

VIRTUALENV := venv-$(PY_VERSION)
VENV_PIP := $(VIRTUALENV)/bin/pip
VENV_PY := $(shell pwd)/$(VIRTUALENV)/bin/python

# This if conditional before the PKG_ variables, so as to ensure we have setuptools;
# not so ideal, but at the moment, I don't know how to do better.  Hmm.
ifeq ($(wildcard $(VENV_PY)),)
	_DUMMY := $(shell $(PYTHON3) -mvenv "$(VIRTUALENV)")
	_DUMMY := $(shell $(VENV_PY) -mpip install -U pip -U setuptools)
endif

# "package name - dash" and "package name - underscore"
PKG_NAME := "globus-compute-agent"
PIP_NAME_D := $(shell cd ../; $(VENV_PY) setup.py --name)
PIP_NAME_U := $(shell echo $(PIP_NAME_D) | tr '-' '_')

PKG_VERSION := $(shell cd ../; $(VENV_PY) setup.py --version | tr '-' '~')
PKG_WHEEL = $(PIP_NAME_U)-$(PKG_VERSION)-py$(PY_MAJOR_VERSION)-none-any.whl
PREREQS_TARBALL_NAME = $(PIP_NAME_U)-prereqs-py$(PY_VERSION)-$(PKG_VERSION).tar.xz

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
	@echo "PIP_NAME_D           : $(PIP_NAME_D)"
	@echo "PIP_NAME_U           : $(PIP_NAME_U)"
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
	rm -rf -- venv-* "$(PIP_NAME_U)"-*tar.xz "$(PIP_NAME_U)"-*whl

_build_needs:
	@/bin/echo -e "\033[36;1;40mChecking build dependencies before starting build\033[m ...\n"
	@[ -x "$$(command -v git)" ] || { echo "'git' not found; missing 'git' package?"; exit 1; }
	@[ -x "$$(command -v xz)" ] || { echo "'xz' not found; missing 'xz' package?"; exit 1; }
	@[ -x "$$(command -v sed)" ] || { echo "'sed' not found; missing 'sed' package?"; exit 1; }
	@[ -x "$$(command -v tar)" ] || { echo "'tar' not found; missing 'tar' package?"; exit 1; }
	@[ -x "$(PYTHON3)" ] || { echo "Python interpreter not found ('$(PYTHON3)'); try 'make show_vars' for insight?"; exit 1; }

.PHONY: $(VENV_PY)
$(VENV_PY): _build_needs
	@if [ "$(PY_MAJOR_VERSION)" -ne 3 ] || [ "$(PY_MINOR_VERSION)" -lt 9 ]; then \
		echo "Unsupported python version $(PY_FULL_VERSION). At least 3.9 is required."; \
		echo "To override python path, use the following"; \
		echo "    make PYTHON3=/path/to/python"; \
		exit 1; \
	fi
	$(PYTHON3) -mvenv $(VIRTUALENV)
	. $(VIRTUALENV)/bin/activate
	@$(VENV_PY) -m pip install -U pip -U setuptools -U build

$(PKG_WHEEL): $(VENV_PY)
	(   rm -rf build/ \
	 && git clone ../../ build/ \
	 && cd build/compute_endpoint/ \
	 && echo -n "    Git Tag: " \
	 && if ! git describe --tags --exact-match; then \
	   { /bin/echo -e "\nBUILD COMPUTE FROM A RELEASE TAG (current branch: $$(git branch --show-current))"; exit 1; } \
	    fi \
	 && rm -rf tests/ \
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

deb_build_needs:  ##-Check that necessary executables are available before starting the DEB build.
	@[ -x "$$(command -v dpkg-checkbuilddeps)" ] || { echo "'dpkg-checkbuilddeps' not found; missing 'dpkg-dev' package?"; exit 1; }
	@dpkg-checkbuilddeps

deb: deb_build_needs dist  ##-Build a Debian package of the Globus Compute Endpoint (.deb)
	(   cd dist/ \
	 && rm -rf debbuild/ \
	 && mkdir -p debbuild/$(PKG_NAME)/wheels/ \
	 && tar -C debbuild/$(PKG_NAME)/wheels/ -xf "$(PREREQS_TARBALL_NAME)" \
	 && cp $(PKG_WHEEL) debbuild/$(PKG_NAME)/wheels/ \
	 && cp -R ../debian debbuild/$(PKG_NAME)/ \
	 && cp ../package_shim.sh debbuild/$(PKG_NAME)/ \
	 && cd debbuild/$(PKG_NAME)/ \
	 && mv debian/changelog.in.in debian/changelog \
	 && sed -i debian/changelog \
	    -e "s/@PACKAGE_NAME@/$(PKG_NAME)/g" \
	    -e "s/@PACKAGE_VERSION@/$(PKG_VERSION)/g" \
	    -e "s/@distro@/$(OS_CODENAME)/g" \
	 && sed -i debian/control \
	    -e "s/@PACKAGE_NAME@/$(PKG_NAME)/g" \
	    -e "s/@PIP_NAME@/$(PIP_NAME_D)/g" \
	 && sed -i debian/copyright -e "s/@PIP_NAME@/$(PIP_NAME_D)/g" \
	 && sed -i debian/rules \
	    -e "s/@PACKAGE_NAME@/$(PKG_NAME)/g" \
	    -e "s/@PACKAGE_VERSION@/$(PKG_VERSION)/g" \
	    -e "s/@PIP_NAME@/$(PIP_NAME_D)/g" \
	 && dpkg-buildpackage -uc -us \
	)
	@echo "\nDEB package successfully built:"
	@ls -lh dist/debbuild/*deb

rpm_build_needs:  ##-Check that necessary executables are available before starting the RPM build.
	@[ -x "$$(command -v rpmbuild)" ] || { echo "'rpmbuild' not found; missing 'rpmdevtools' or 'rpm-build' package(s)?"; exit 1; }

rpm: rpm_build_needs dist  ##-Build an RPM package of the Globus Compute Endpoint (.rpm)
	(   cd dist/ \
	 && pwd && ls \
	 && rm -rf rpmbuild/ \
	 && mkdir -p rpmbuild/{BUILD,BUILDROOT,RPMS,SPECS,SRPMS} rpmbuild/SOURCES/$(PKG_NAME)/wheels \
	 && tar -C rpmbuild/SOURCES/$(PKG_NAME)/wheels -xf "$(PREREQS_TARBALL_NAME)" \
	 && cp $(PKG_WHEEL) rpmbuild/SOURCES/$(PKG_NAME)/wheels/ \
	 && cp ../package_shim.sh rpmbuild/SOURCES/$(PKG_NAME)/ \
	 && sed \
	    -e "s/@PACKAGE_VERSION@/$(PKG_VERSION)/g" \
	    -e "s/@PACKAGE_NAME@/$(PKG_NAME)/g" \
	    -e "s/@PACKAGE_WHEEL@/$(PKG_WHEEL)/g" \
	    -e "s/@PIP_NAME@/$(PIP_NAME_D)/g" \
	    < ../fedora/$(PKG_NAME).spec.in > ./$(PKG_NAME).spec \
	 && HOME="$$(pwd)" rpmbuild --define "_topdir $$(pwd)/rpmbuild" -ba ./$(PKG_NAME).spec \
	)
	@echo -e "\nRPM package successfully built:"
	@ls -lh dist/rpmbuild/RPMS/**/*rpm
