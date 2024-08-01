---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior, for e.g:
1. Install globus-compute-sdk==2.0.0 and globus-compute-endpoint==2.0.0 with Python 3.10 on cluster
2. Run a test script
3. Wait 5 mins
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Environment**
 - OS: [e.g. ubuntu, centos, MacOS, windows] @ client
 - OS & Container technology: [e.g. ubuntu, centos & singularity, docker] @ endpoint
 - Python version @ client
 - Python version @ endpoint
 - globus-compute-sdk version @ client
 - globus-compute-endpoint version @ endpoint

**Distributed Environment**
- Where are you running the funcX script from? [e.g. Laptop/Workstation, Login node, Compute node]
- Where does the endpoint run? [e.g. Laptop/Workstation, Login node]
- What is your endpoint-uuid?
- If this is an endpoint issue, run `globus-compute-endpoint self-diagnostic -z` and attach the resulting zip file.
  This archive will contain logs, configuration, and machine information; if you'd prefer to share it privately,
  you can reach the [Compute team via Slack](https://join.slack.com/t/funcx/shared_invite/zt-gfeclqkz-RuKjkZkvj1t~eWvlnZV0KA).
