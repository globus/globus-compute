#!/usr/bin/env python
"""
A script which runs as part of our GitHub Actions build to send notifications
to Slack. Notifies when build status changes.

Usage:
    ./github-slack-notify.py $STATUS

where $STATUS is the current build status.
Requires GITHUB_TOKEN and SLACK_WEBHOOK_URL to be in the environment.
"""
import os
import sys

import requests


def statusemoji(status):
    return {"success": ":heavy_check_mark:", "failure": ":rotating_light:"}.get(
        status, ":warning:"
    )


def get_message_title():
    return os.getenv("SLACK_MESSAGE_TITLE", "GitHub Actions Tests")


def get_build_url():
    return "https://github.com/{GITHUB_REPOSITORY}/commit/{GITHUB_SHA}/checks".format(
        **os.environ
    )


def is_pr_build():
    return os.environ["GITHUB_REF"].startswith("refs/pull/")


def get_branchname():
    # split on slashes, strip 'refs/heads/*' , and rejoin
    # this is also the tag name if a tag is used
    return "/".join(os.environ["GITHUB_REF"].split("/")[2:])


def _get_workflow_id_map():
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GITHUB_TOKEN"]
    r = requests.get(
        f"https://api.github.com/repos/{repo}/actions/workflows",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
        },
    )

    ret = {}
    for w in r.json().get("workflows", []):
        ret[w["name"]] = w["id"]
    print(f">> workflow IDs: {ret}")
    return ret


def get_last_build_status():
    branch = get_branchname()
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GITHUB_TOKEN"]
    workflow_id = _get_workflow_id_map()[os.environ["GITHUB_WORKFLOW"]]

    r = requests.get(
        f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_id}/runs",
        params={"branch": branch, "status": "completed", "per_page": 1},
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.v3+json",
        },
    )
    print(f">> get past workflow runs params: branch={branch},repo={repo}")
    print(f">> get past workflow runs result: status={r.status_code}")
    runs_docs = r.json().get("workflow_runs", [])
    # no suitable status was found for a previous build, so the status is "None"
    if not runs_docs:
        print(">>> no previous run found for workflow")
        return None
    conclusion = runs_docs[0]["conclusion"]
    print(f">>> previous run found with conclusion={conclusion}")
    return conclusion


def check_status_changed(status):
    # NOTE: last_status==None is always considered a change. This is intentional
    last_status = get_last_build_status()
    res = last_status != status
    if res:
        print(f"status change detected (old={last_status}, new={status})")
    else:
        print(f"no status change detected (old={last_status}, new={status})")
    return res


def get_failure_message():
    return os.getenv("SLACK_FAILURE_MESSAGE", "@channel tests failed")


def build_payload(status):
    context = f"{statusemoji(status)} build for {get_branchname()}: {status}"
    message = f"<{get_build_url()}|{get_message_title()}>"
    if "fail" in status.lower():
        message = f"{message}: {get_failure_message()}"

    return {
        "channel": os.getenv("SLACK_CHANNEL", "#testing"),
        "username": "github-actions",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": message}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": context}]},
        ],
    }


def on_main_repo():
    """check if running from a fork"""
    res = os.environ["GITHUB_REPOSITORY"].lower() == "funcx-faas/funcx"
    print(f"Checking main repo: {res}")
    return res


def should_notify(status):
    res = check_status_changed(status) and on_main_repo() and not is_pr_build()
    print(f"Should notify: {res}")
    return res


def main():
    if len(sys.argv) != 2:
        sys.exit(2)
    status = sys.argv[1]
    if should_notify(status):
        r = requests.post(os.environ["SLACK_WEBHOOK_URL"], json=build_payload(status))
        print(f">> webhook response: status={r.status_code}")
        print(r.text)


if __name__ == "__main__":
    main()
