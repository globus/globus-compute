from urllib.parse import urlparse


def ws_uri_from_service_address(service_address):
    url_data = urlparse(service_address)
    scheme = "wss" if url_data.scheme == "https" else "ws"
    hostname = url_data.hostname
    port_str = "" if url_data.port is None else f":{url_data.port}"
    ws_uri = f"{scheme}://{hostname}{port_str}/ws/v2/"
    return ws_uri
