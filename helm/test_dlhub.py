from dlhub_sdk.client import DLHubClient

dl = DLHubClient()


user = "zhuozhao_uchicago"
name = "noop"
data = {"data": ["V", "Co", "Zr"]}

# Test a synchronous request
res = dl.run("{}/{}".format(user, name), data, timeout=60)
print(res)
assert res == 'Hello'
