# Contributing Guide

This doc covers dev setup and guidelines for contributing.

FIXME: This doc is a stub.

## Requirements

- python3.7+, pip, virtualenv

### Recommended

- [pre-commit](https://pre-commit.com/)
- [scriv](https://scriv.readthedocs.io/en/latest/index.html)

## Linting

Linting can be run via pre-commit. Run for all files in the repo:

    pre-commit run -a

### (Optional) Setup pre-commit Hooks

For the best development experience, set up linting and autofixing pre-commit
git hooks using the `pre-commit` tool.

After installing `pre-commit`, run

    pre-commit install

in the repo to configure hooks.

> NOTE: If necessary, you can always skip hooks with `git commit --no-verify`

## Adding Changelog Fragments

Any change to the codebase must either include a changelog fragment (in some
projects these are called "newsfiles") or be in a GitHub PR with the label
`no-news-is-good-news`.

To create a new changelog fragment, run

    scriv create --edit

and populate the fragment. It will include comments which instruct you on how
to fill out the fragment.

## Installing Testing Requirements

Testing requirements for each of the two packages in this repository
(globus-compute-sdk and globus-compute-endpoint) are specified as installable extras.

To install the globus-compute-sdk test requirements

    cd compute_sdk
    pip install '.[test]'

To install the globus-compute-endpoint test requirements

    cd compute_endpoint
    pip install '.[test]'
