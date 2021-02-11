from funcx.sdk.client import FuncXClient


if __name__ == "__main__":

    fxc = FuncXClient()
    print(fxc)

    fxc.register_endpoint('foobar', None)
