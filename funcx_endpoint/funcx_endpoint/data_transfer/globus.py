import logging
import os

import globus_sdk
from fair_research_login import JSONTokenStorage, NativeClient

logger = logging.getLogger("interchange")


CLIENT_ID = "a0cb81f7-757e-4564-ac93-7fef03368d53"
TOKEN_LOC = os.path.expanduser("~/.funcx/credentials/funcx_sdk_tokens.json")


class GlobusTransferClient:
    """
    All communication with the Globus Auth and Globus Transfer services is enclosed
    in the Globus class. In particular, the Globus class is reponsible for:
     - managing an OAuth2 authorizer - getting access and refresh tokens,
       refreshing an access token, storing to and retrieving tokens from
       .globus.json file,
     - submitting file transfers,
     - monitoring transfers.
    """

    def __init__(
        self,
        local_path=".",
        dst_ep=None,
        funcx_ep_id=None,
        sync_level="checksum",
        **kwargs,
    ):
        """Initialize a globus transfer client

        Parameters
        ----------
        local_path: str
        The local path to store the data

        dst_ep: stri
        The destination endpoint id

        """
        transfer_scope = "urn:globus:auth:scope:transfer.api.globus.org:all"
        scopes = [transfer_scope]
        self.native_client = NativeClient(
            client_id=CLIENT_ID,
            app_name="funcX data transfer",
            token_storage=JSONTokenStorage(TOKEN_LOC),
        )

        self.native_client.login(
            requested_scopes=scopes,
            no_local_server=True,
            no_browser=True,
            refresh_tokens=True,
        )

        transfer_authorizer = self.native_client.get_authorizers_by_scope(
            requested_scopes=scopes
        )[transfer_scope]
        self.transfer_client = globus_sdk.TransferClient(transfer_authorizer)

        self.local_path = local_path
        os.makedirs(self.local_path, exist_ok=True)
        logger.info(f"Local globus data path {self.local_path} created.")
        self.dst_ep = dst_ep
        self.funcx_ep_id = funcx_ep_id

        self.sync_level = sync_level
        logger.info(
            "Initiated Globus transfer client for EP {} with local data path {}".format(
                self.dst_ep, self.local_path
            )
        )

    def transfer(self, src_ep, src_path, basename, recursive=False):
        tdata = globus_sdk.TransferData(
            self.transfer_client,
            src_ep,
            self.dst_ep,
            label=f"Transfer on funcX Endpoint {self.funcx_ep_id}",
            sync_level=self.sync_level,
        )

        dst_path = f"{self.local_path}/{basename}"
        tdata.add_item(src_path, dst_path, recursive=recursive)
        try:
            task = self.transfer_client.submit_transfer(tdata)
            logger.info(
                "Submitted Globus transfer from {}{} to {}{}".format(
                    src_ep, src_path, self.dst_ep, self.local_path
                )
            )
            logger.info(f"Task info: {task}")
        except Exception as e:
            logger.exception(
                "Globus transfer from {}{} to {}{} failed due to error: {}".format(
                    src_ep, src_path, self.dst_ep, self.local_path, e
                )
            )
            raise Exception(
                "Globus transfer from {}{} to {}{} failed due to error: {}".format(
                    src_ep, src_path, self.dst_ep, self.local_path, e
                )
            )

        return (task, src_ep, src_path, self.dst_ep, dst_path)

    def status(self, task):
        task_id = task["task_id"]
        status = self.transfer_client.get_task(task_id)
        return status

    def get_event(self, task):
        task_id = task["task_id"]
        events = self.transfer_client.task_event_list(
            task_id, num_results=1, filter="is_error:1"
        )
        try:
            event = events.data[0]["details"]
            return event
        except IndexError:
            logger.debug(f"No globus transfer error for task {task_id}")
            return
        except Exception:
            logger.exception(
                "Got exception when fetching globus transfer "
                "error event for task {}".format(task_id)
            )
            return

    def cancel(self, task):
        task_id = task["task_id"]
        res = self.transfer_client.cancel_task(task_id)
        logger.info(f"Canceling task {task_id}, got message: {res}")
        if res["code"] != "Canceled":
            logger.error(
                "Could not cancel task {}. Reason: {}".format(task_id, res["message"])
            )
