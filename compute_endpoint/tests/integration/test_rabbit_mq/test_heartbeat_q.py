from urllib import parse

import pytest


@pytest.mark.parametrize("use_heartbeat", [None, 1])
def test_no_heartbeat(
    start_heartbeat_q_publisher,
    rabbitmq_conn_url,
    create_result_queue_info,
    use_heartbeat,
):
    """Confirm that result_q_publisher does not disconnect when delay
    between messages exceed heartbeat period
    """
    url_parts = list(parse.urlparse(rabbitmq_conn_url))
    query_parts = parse.parse_qs(url_parts[4])
    query_parts.pop("heartbeat", None)
    if use_heartbeat:
        query_parts["heartbeat"] = use_heartbeat
    url_parts[4] = parse.urlencode(query_parts)
    conn_url = parse.urlunparse(url_parts)

    q_info = create_result_queue_info(connection_url=conn_url)

    hb_pub = start_heartbeat_q_publisher(override_params=q_info)

    # simply ensure no crash between two message sends
    f = hb_pub.publish(b"Hello test_no_heartbeat: 1")
    f.result(timeout=5)
    f = hb_pub.publish(b"Hello test_no_heartbeat: 2")
    f.result(timeout=5)


def test_reconnect_after_disconnect(start_heartbeat_q_publisher):
    hb_pub = start_heartbeat_q_publisher()
    f = hb_pub.publish(b"Hello test_reconnect_after_disconnect: before")
    f.result(timeout=5)
    hb_pub._mq_chan.close()
    f = hb_pub.publish(b"Hello test_reconnect_after_disconnect: afterward")
    f.result(timeout=5)
