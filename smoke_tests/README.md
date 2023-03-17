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

As with the above make targets, these are just wrappers around tox; can do the above by invoking tox direcly:

    tox -- --compute-config local
    tox -e localdeps -- --compute-config local
