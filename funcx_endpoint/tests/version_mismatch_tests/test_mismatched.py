import argparse
from funcx import FuncXClient
from funcx.sdk.executor import FuncXExecutor


def get_version():
    import sys
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def test_worker_version(fx, ep_id, ep_version, version):
    import sys
    print(f"Running a version check against endpoint:{ep_id}")
    future = fx.submit(get_version, endpoint_id=ep_id)
    print(f"Future launched with future:{future}")
    try:
        print(f"Expected worker_version : {version}, actual: {future.result()}")
        assert future.result(timeout=10) == version, \
            f"Expected worker version:{version} Got:{future.result()}"

    except Exception:
        print(f"Expected worker_version : {version}, actual: {future.result()}")
        sys.exit(-1)
    else:
        print(f"Worker returned the expected version:{future.result()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--endpoint_id",
        required=True,
        help="Target endpoint to send functions to",
    )
    parser.add_argument(
        "-v",
        "--ep_version",
        required=True,
        help="EP VERSION",
    )
    parser.add_argument(
        "-w",
        "--worker_version",
        required=True,
        help="Target endpoint to send functions to",
    )

    args = parser.parse_args()

    fx = FuncXExecutor(FuncXClient())
    test_worker_version(fx, args.endpoint_id, args.ep_version, args.worker_version)
