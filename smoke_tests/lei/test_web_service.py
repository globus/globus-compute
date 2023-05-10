from datetime import datetime
import sys
import time
import uuid
from globus_compute_sdk import Client, Executor

TEST_PROD = False
TEST_DEV= True

LOOP_COUNT = 1
WAIT_S = 7 # Sleep function wait

REGISTER_NEW_FUNC = False
# REGISTER_NEW_FUNC = True

DO_EXCEPTION_FUNC = False
DO_NORMAL_FUNC = False
DO_SLEEP_FUNC = True
DO_SLEEP_COUNT = 1

# PROD_EXC_FID = '69791dcb-9aec-4835-9d8a-cca054d7326d'  # lei514@gmail.com
PROD_EXC_FID = 'a4406f99-7991-4a3e-ab78-7fe4a265a1d1'  # lei@globus.org
# PROD_NORMAL_FID = 'e2c4a130-1b7c-413a-b347-32f0d84d2308'  # lei514@gmail.com
# PROD_NORMAL_FID = '59f78cf1-ccd6-46ea-86cb-78d3d4c2ba97'  # lei514@gmail.com
PROD_NORMAL_FID = 'ce0e7300-ba49-4fc9-be63-c5c99542f394'  # lei@globus.org
PROD_SLEEP_FID = 'd7238eab-1dec-4ab0-a0b9-8710821c92e5'  # lei@globus.org
WAIT_FOR_SLEEP_RESULTS = True

# DEV_ARG_KWARG_FID = 'e8b9566d-f50a-4339-8639-35e81069e6cb'

DEV_EXC_FID = '02017f9c-377a-4d0a-988f-797bd5babda3'
DEV_NORMAL_FID = '94c7ce20-7a5a-4eb3-ac07-6fc0ab8ad50c'
DEV_KW_FID = '7b6cff6e-0d49-4376-87a0-dd649272f0b4'
DEV_SLEEP_FID = 'aab66753-4b02-4162-a9d3-47374ddcacc4'  # lei@globus.org

DEV_TUTORIAL_EID = '2238617a-8756-4030-a8ab-44ffb1446092'
PROD_TUTORIAL_EID = '4b116d3c-1703-4f8f-9f6f-39921e5864df'

# PROD_TUTORIAL_EID = 'c2265df4-91be-4c3d-aefc-a1ddd9017d57'  # wlist4
# PROD_TUTORIAL_EID = 'a300d3f1-3e14-4341-83d4-dd5382ce78f4'  # a0test


def value_error_func(s):
  raise ValueError(f"Expected error with input ({s})")


def hello_func(s):
  return "hello " + str(s)


def sleep_func(sleep_s: float):
  import time
  time.sleep(sleep_s)
  return f"Slept {sleep_s} second(s)"


def get_gc_client(is_dev, env_str):
    gcc = Client(environment='dev' if is_dev else None)
    ac = gcc.login_manager.get_auth_client()
    
    user_uuid = ac.oauth2_userinfo()["sub"]
    username = ac.get_identities(ids=user_uuid)["identities"][0]["username"]
    
    print(f"Testing on {env_str} as {username}")
    return gcc


def run_with(is_exc, gcc, eid, fid, val, wait_s: float = 0, run_count: int = 1):
    if not fid:
        return
    if run_count == 1:
        try:
            resp = gcc.run(val, endpoint_id=eid, function_id=fid)
        except Exception as e:
            print(f"  ERROR! Run with fid {fid} failed with: {e}")
            return
        try:
            time.sleep(0.2 + wait_s)
            result = gcc.get_result(resp)
            if not is_exc:
                if str(val) in result:
                    print(f"  OK: Task {resp} correct results")
                else:
                    print(f"  ERROR! Task {resp} results unexpected: {result}")
        except Exception as e:
            e_msg = str(e)
            if not is_exc:
                print(f"  ERROR! Task {resp} failed unexpectedly: {e_msg}")
            else:
                if 'Expected error' in e_msg and val in e_msg:
                    print(f"  OK: Task {resp} failed with expected exception")
                else:
                    print(f"  ERROR! Task {resp} failed unexpectedly: {e_msg}")
    else:
        # Supports sleep/normal functions only
        assert run_count > 1
        batch = gcc.create_batch()
        for i in range(run_count):
            batch.add(fid, eid, [val])
        task_uuids = gcc.batch_run(batch)
        wait_time = val * run_count + 0.2
        if not WAIT_FOR_SLEEP_RESULTS:
            print(f"Submitted {run_count} tasks with ids {task_uuids[:2]}...")
            return
        print(f"Starting to wait, now {datetime.now()}")
        for i in range(run_count):
            time.sleep(val)
            print(f"Waiting, now {datetime.now()}")
        time.sleep(5)
        cur_task = None
        try:
            for i, task_uuid in enumerate(task_uuids):
                cur_task = task_uuid
                result = gcc.get_result(task_uuid)
                if str(val) in result:
                    print(f"  OK: Task #{i+1} {task_uuid} has correct results")
                else:
                    print(f"  ERROR! Task #{i+1} {task_uuid} results unexpected: {result}")
        except Exception as e:
            e_msg = str(e)
            if not is_exc:
                print(f"  ERROR! Task {cur_task} failed unexpectedly: {e_msg}")
            else:
                if 'Expected error' in e_msg and val in e_msg:
                    print(f"  OK: Task {cur_task} failed with expected exception")
                else:
                    print(f"  ERROR! Task {cur_task} failed unexpectedly: {e_msg}")

        return

    gce = Executor(eid, funcx_client=gcc)
    try:
        print("  Submitting to Executor...")
        fut = gce.submit_to_registered_function(fid, (val,))
        time.sleep(0.2 + wait_s)
        result = fut.result()
        if is_exc:
            print("    ERROR! Executor task did not raise as expected!!!")
        else:
            if str(val) in result:
                print(f"  OK: Task {resp} Executor results correct")
            else:
                print(f"  ERROR! Task {resp} Executor results unexpected: {result}")
    except Exception as e:
        e_msg = str(e)
        if is_exc:
            if 'Expected error' in e_msg and val in e_msg:
                print(f"    OK: Executor Task {resp} failed with expected exception")
            else:
                print(f"    ERROR! Executor Task {resp} failed unexpectedly: {e_msg}")
        else:
            print(f"  ERROR! Task {resp} failed Executor unexpectedly: {e_msg}")


def test_run(dev=False, endpoint_uuid=None):
    env_str = '<dev>' if dev else '<prod>'
    gcc = get_gc_client(dev, env_str)
    if endpoint_uuid is None:
        endpoint_uuid = DEV_TUTORIAL_EID if dev else PROD_TUTORIAL_EID

    if DO_EXCEPTION_FUNC:
        if REGISTER_NEW_FUNC:
            fid = gcc.register_function(value_error_func)
            print(f"  Registered exception function {fid}")
            run_with(True, gcc, endpoint_uuid, fid, f"fid {fid}")

        fid = DEV_EXC_FID if dev else PROD_EXC_FID
        print(f"  Using existing exception function {fid}")
        run_with(True, gcc, endpoint_uuid, fid, f"fid {fid}")

    if DO_NORMAL_FUNC:
        if REGISTER_NEW_FUNC:
            fid = gcc.register_function(hello_func)
            print(f"  Registered normal function {fid}")
            run_with(False, gcc, endpoint_uuid, fid, f"fid {fid}")

        fid = DEV_NORMAL_FID if dev else PROD_NORMAL_FID
        print(f"  Using existing normal function {fid}")
        run_with(False, gcc, endpoint_uuid, fid, f"fid {fid}")

    if DO_SLEEP_FUNC:
        if REGISTER_NEW_FUNC:
            fid = gcc.register_function(sleep_func)
        else:
            fid = DEV_SLEEP_FID if dev else PROD_SLEEP_FID
        print(f"  Using existing sleep function {fid}")
        run_with(
            False,
            gcc,
            endpoint_uuid,
            fid,
            WAIT_S,
            wait_s=WAIT_S,
            run_count=DO_SLEEP_COUNT
        )

    print(f"Completed testing {env_str}")


def is_uuid(s):
    try:
        uuid.UUID(s)
        return True
    except ValueError:
        return False


endpoint_to_run = None

for arg in sys.argv[1:]:
    if 'prod' == arg.lower():
        TEST_PROD = True
    elif 'dev' == arg.lower():
        TEST_DEV = True
    elif arg.isdigit():
        LOOP_COUNT = int(arg)
    elif is_uuid(arg):
        if TEST_PROD == TEST_DEV:
            raise ValueError("If specifying an endpoint UUID, only one of dev or prod can be set")
        else:
            endpoint_to_run = arg
        
if not TEST_PROD and not TEST_DEV:
    raise ValueError("Must specify at least one of prod/dev")

for i in range(LOOP_COUNT):
    if TEST_PROD:
        test_run(endpoint_uuid=endpoint_to_run)
    if TEST_DEV:
        test_run(dev=True, endpoint_uuid=endpoint_to_run)
