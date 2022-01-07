from urllib.parse import urlparse

from funcx.utils.errors import InvalidServiceAddress


def validate_service_address(service_address):
    try:
        url_data = urlparse(service_address)
        # port must be accessed to raise port value issues
        url_data.port
        if url_data.scheme != "http" and url_data.scheme != "https":
            raise InvalidServiceAddress("Protocol must be HTTP/HTTPS")

        if url_data.netloc is None or url_data.hostname is None:
            raise InvalidServiceAddress("Address is malformed")
    except Exception as e:
        raise InvalidServiceAddress(f"Address is malformed - {e}")


def ws_uri_from_service_address(service_address):
    url_data = urlparse(service_address)
    scheme = "wss" if url_data.scheme == "https" else "ws"
    hostname = url_data.hostname
    port = url_data.port
    port_str = ""
    # for testing purposes: the service address is typically
    # localhost:5000 and ws uri is localhost:6000
    if port == 5000:
        port = 6000
    port_str = "" if port is None else f":{port}"
    ws_uri = f"{scheme}://{hostname}{port_str}/ws/v2/"
    return ws_uri
