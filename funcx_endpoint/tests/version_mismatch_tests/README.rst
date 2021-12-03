Setting up the envs
-------------------

To run these tests first create 4 conda envs with the appropriate python3 versions

```bash
funcx_version_mismatch_py3.6
funcx_version_mismatch_py3.7
funcx_version_mismatch_py3.8
funcx_version_mismatch_py3.9
```

Next checkout the branch `relax_version_match_constraints` and run the `update_all.sh` script
to install the locally checked out code. Run the `update_all.sh` script like this:

```bash

./update_all.sh <PATH_TO_FUNCX_REPO>
```

Create an endpoint
------------------

You need an endpoint running locally named `mismatched`

```
funcx-endpoint configure mismatched
```

You do not need to start, or configure this EP. The tests below will copy over configs.

Running the tests
-----------------

Run the tests like this:

```
bash -i $PWD/run_test_matrix.sh $PWD
```




