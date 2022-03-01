import globus_sdk


class FuncxAPIError(globus_sdk.GlobusAPIError):
    """
    An error raised by the FuncXClient when the web API responds with an error.
    """

    MESSAGE_FIELDS = ["reason", "message"]

    @property
    def code_name(self) -> str:
        """
        A friendly string name for the error code which was recieved, or
        ``"UNKNOWN"`` if the code is not recognized.
        """
        if self.raw_json is None:
            raise ValueError("could not JSON decode response")

        try:
            code_int = self.raw_json["code"]
        except KeyError as e:
            raise ValueError("Could not get 'code' from error response") from e

        return {
            1: "user_unauthenticated",
            2: "user_not_found",
            3: "function_not_found",
            4: "endpoint_not_found",
            5: "container_not_found",
            6: "task_not_found",
            7: "auth_group_not_found",
            8: "function_access_forbidden",
            9: "endpoint_access_forbidden",
            10: "function_not_permitted",
            11: "endpoint_already_registered",
            12: "forwarder_registration_error",
            13: "forwarder_contact_error",
            14: "endpoint_stats_error",
            15: "liveness_stats_error",
            16: "request_key_error",
            17: "request_malformed",
            18: "internal_error",
            19: "endpoint_outdated",
            20: "task_group_not_found",
            21: "task_group_access_forbidden",
            22: "invalid_uuid",
        }.get(code_int, "UNKNOWN")
