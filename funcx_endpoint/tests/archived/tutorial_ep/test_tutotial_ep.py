import argparse
import copy
import logging
import sys
import time

from globus_sdk import AccessTokenAuthorizer, ConfidentialAppAuthClient

from funcx.sdk.client import FuncXClient


def identity(x):
    return x


class TestTutorial:
    def __init__(
        self,
        fx_auth,
        search_auth,
        openid_auth,
        endpoint_id,
        func,
        expected,
        args=None,
        timeout=15,
        concurrency=1,
        tol=1e-5,
    ):
        self.endpoint_id = endpoint_id
        self.func = func
        self.expected = expected
        self.args = args
        self.timeout = timeout
        self.concurrency = concurrency
        self.tol = tol
        self.fxc = FuncXClient(
            fx_authorizer=fx_auth,
            search_authorizer=search_auth,
            openid_authorizer=openid_auth,
        )
        self.func_uuid = self.fxc.register_function(self.func)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s %(name)s:%(lineno)d [%(levelname)s]  %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def run(self):
        try:
            submissions = []
            for _ in range(self.concurrency):
                task = self.fxc.run(
                    self.args, endpoint_id=self.endpoint_id, function_id=self.func_uuid
                )
                submissions.append(task)

            time.sleep(self.timeout)

            unfinished = copy.deepcopy(submissions)
            while True:
                unfinished[:] = [
                    task for task in unfinished if self.fxc.get_task(task)["pending"]
                ]
                if not unfinished:
                    break
                time.sleep(self.timeout)

            success = 0
            for task in submissions:
                result = self.fxc.get_result(task)
                if abs(result - self.expected) > self.tol:
                    self.logger.exception(
                        f"Difference for task {task}. "
                        f"Returned: {result}, Expected: {self.expected}"
                    )
                else:
                    success += 1

            self.logger.info(
                f"{success}/{self.concurrency} tasks completed successfully"
            )
        except KeyboardInterrupt:
            self.logger.info("Cancelled by keyboard interruption")
        except Exception as e:
            self.logger.exception(f"Encountered exception: {e}")
            raise


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tutorial", required=True, help="Tutorial Endpoint ID")
    parser.add_argument("-i", "--id", required=True, help="API_CLIENT_ID for Globus")
    parser.add_argument(
        "-s", "--secret", required=True, help="API_CLIENT_SECRET for Globus"
    )
    args = parser.parse_args()

    client = ConfidentialAppAuthClient(args.id, args.secret)
    scopes = [
        "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
        "urn:globus:auth:scope:search.api.globus.org:all",
        "openid",
    ]

    token_response = client.oauth2_client_credentials_tokens(requested_scopes=scopes)
    fx_token = token_response.by_resource_server["funcx_service"]["access_token"]
    search_token = token_response.by_resource_server["search.api.globus.org"][
        "access_token"
    ]
    openid_token = token_response.by_resource_server["auth.globus.org"]["access_token"]

    fx_auth = AccessTokenAuthorizer(fx_token)
    search_auth = AccessTokenAuthorizer(search_token)
    openid_auth = AccessTokenAuthorizer(openid_token)

    val = 1
    tt = TestTutorial(
        fx_auth, search_auth, openid_auth, args.tutorial, identity, val, args=val
    )
    tt.run()
