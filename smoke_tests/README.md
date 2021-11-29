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

    tox -e localdeps -- --funcx-config dev
