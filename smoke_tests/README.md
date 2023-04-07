# smoke tests

Run a set of standard tests to ensure that the service and client code work
together.

## Requirements

- make
- tox

## Running the tests

To run tests on prod:

    make prod

To run tests on dev:

    make dev

To run using local dependency versions (install SDK and endpoint from the
repo):

    tox -e localdeps

You can also run `localdeps` against dev with

    tox -e localdeps -- --compute-config dev

One can also run tests against a local webservice setup.  Use the make targets
`local_with_published_sdk` and `local_with_dev_sdk` to run tests with published
(to PyPi) SDK code, or with the local (possibly not even committed) dev code:

    make local_with_published_sdk
    make local_with_dev_sdk

As with the above make targets, these are just wrappers around tox; can do the
above by invoking tox direcly:

    tox -- --compute-config local
    tox -e localdeps -- --compute-config local

## Non-Pytest tests

For those tests that require "raw" access to resources -- without the harness
afforded by Pytest -- we have implemented a simple shell-script runner in
`tests/sh/`.  To add new tests, create a shell-script in that directory that is
prefixed with `test_`, suffixed with `.sh`, and is executable.  For example:

    touch tests/sh/test_cool_new_test.sh
    chmod +x tests/sh/test_cool_new_test.sh

These tests are completely independent of any harness, so it is more incumbent
upon the developer (than usual) to ensure that the tests clean up after
themselves.

For running the non-Pytests without running the other tests, invoke the
`runner.sh` file (to run them all), or invoke the specific test of interest
directly:

    tests/sh/runner.sh           # invoke them all
    tests/sh/test_endpoint.sh    # invoke a single test
