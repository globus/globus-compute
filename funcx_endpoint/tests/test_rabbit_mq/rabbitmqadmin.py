from __future__ import annotations

import logging
import os
import typing as t
from urllib.parse import quote_plus

import requests


class RabbitMQAdmin:
    """
    A wrapper around the RabbitMQ administrative API.[1]  Only a small number of the
    admin API endpoints are implemented.

    The class requires either an explicit baseuri during instantiation, or the
    environment variable RABBITMQ_ADMIN_URI to be set.  Example:

        >>> os.environ["RABBITMQ_ADMIN_URI"]
        'http://adminuser:adminpass@localhost:15672'
        >>> rmqa = RabbitMQAdmin()

    Alternatively, the baseuri may be specified manually:

        >>> rmqa = RabbitMQAdmin(baseuri="http://adminuser:adminpass@localhost:15672")

    All methods internally use the `requests` module, and raise_for_status().

    [1] rabbitmq_fqdn:15672/api/index.html ; also linked from
        https://www.rabbitmq.com/management.html#http-api
    """

    def __init__(self, baseuri: str | None = None):
        if not baseuri:
            baseuri = os.environ["RABBITMQ_ADMIN_URI"]
            if not baseuri:
                raise ValueError("RABBITMQ_ADMIN_URI must be a valid connection url")
        self.baseuri = baseuri.rstrip("/") + "/api"

    @staticmethod
    def _request(method: str, url, **kwargs):
        res = requests.request(method, url, **kwargs)
        try:
            res.raise_for_status()
        except Exception as exc:
            # At least one consumer of this method intentionally ignores
            # a particular failure, so only emit if actively debugging.  No
            # sense in needlessly catching attention of log trawlers.
            logging.debug(
                f"Failed to {method.upper()}.  Exception text: {exc}",
                extra={
                    "url": f"{res.request.url}",
                    "headers": f"{res.request.headers}",
                    "body": f"{res.request.body!r}",
                },
            )
            raise
        return res

    def _post(self, path: str, payload):
        url = self.baseuri + path
        return self._request("post", url, json=payload)

    def _put(self, path: str, payload):
        url = self.baseuri + path
        return self._request("put", url, json=payload)

    def _get(self, path: str):
        url = self.baseuri + path
        return self._request("get", url)

    def _delete(self, path: str):
        url = self.baseuri + path
        return self._request("delete", url)

    def list_connections(self) -> list:
        response = self._get("/connections")
        return response.json()

    def delete_connections(self) -> int:
        """Deletes active connections and returns count of connections deleted"""
        connections = self.list_connections()
        count = 0
        for conn in connections:
            conn_name = conn["name"]
            logging.warning(f"Trying to delete conn: {conn_name}")
            self._delete(f"/connections/{conn_name}")
            count += 1
        return count

    def delete_users(self, usernames: t.Iterable[str]) -> requests.Response:
        """
        Implements POST /api/users/bulk-delete

        Parameters
        ----------
        usernames : Iterable[str]
            A list of RMQ usernames

        Returns
        -------
        request.Response
        """
        api_endpoint = "/users/bulk-delete"
        payload = {"users": list(usernames)}
        return self._post(api_endpoint, payload=payload)

    def create_user(
        self,
        username: str,
        password: str | None = None,
        password_hash: str | None = None,
        tags: list[str] | None = None,
    ) -> requests.Response:
        """
        Implements PUT /api/users/<username>

        Parameters
        ----------
        username : str
            A username for RMQ to create
        password : str | None
            (Optional) A password for the new RMQ user (xor `password_hash`)
        password_hash: str | None
            (Optional) A password has for the new RMQ user (xor `password`)
        tags : list[str] | None
            (Optional) List of RMQ user tags

        Returns
        -------
        request.Response
        """
        api_endpoint = f"/users/{username}"
        payload = {}
        if password_hash is not None:
            payload["password_hash"] = password_hash
        elif password:
            payload["password"] = password

        payload["tags"] = tags and ",".join(map(str, tags)) or ""

        return self._put(api_endpoint, payload=payload)

    def set_user_vhost_permissions(
        self,
        username: str,
        vhost: str = "/",
        configure_re: str = r"^$",
        write_re: str = r"^$",
        read_re: str = r"^$",
    ) -> requests.Response:
        """
        Implements PUT /api/permissions/<vhost>/<username>

        For more information on specific parameters, consult the official RMQ
        documentation.  Reference the Admin API documentation and
            https://www.rabbitmq.com/access-control.html#authorisation

        Parameters
        ----------
        username : str
            An RMQ username
        vhost : str (default: "/")
            Operate on selected vhost for username
        configure_re: str (default: "^$")
            The configuration regex (see RMQ documentation for more info)
        write_re : str (default: "^$")
            The write regex (see RMQ documentation for more info)
        read_re : str (default: "^$")
            The read regex (see RMQ documentation for more info)

        Returns
        -------
        request.Response
        """
        vhost = quote_plus(vhost)
        api_endpoint = f"/permissions/{vhost}/{username}"
        payload = {"configure": configure_re, "write": write_re, "read": read_re}
        return self._put(api_endpoint, payload=payload)

    def set_user_vhost_topic_permissions(
        self,
        username: str,
        topic: str,
        vhost: str = "/",
        write_re: str = "^$",
        read_re: str = "^$",
    ) -> requests.Response:
        """
        Implements PUT /api/topic-permissions/<vhost>/<username>

        Parameters
        ----------
        username : str
            An RMQ username
        topic: str,
            Also called "exchange name"
        vhost : str (default: "/")
            Operate on selected vhost for username and topic
        write_re : str (default: "^$")
            The write regex (see RMQ documentation for more info)
        read_re : str (default: "^$")
            The read regex (see RMQ documentation for more info)

        Returns
        -------
        request.Response
        """
        vhost = quote_plus(vhost)
        api_endpoint = f"/topic-permissions/{vhost}/{username}"
        payload = {"exchange": topic, "write": write_re, "read": read_re}
        return self._put(api_endpoint, payload=payload)


if __name__ == "__main__":

    rmq_admin = RabbitMQAdmin()

    print(rmq_admin.list_connections())
