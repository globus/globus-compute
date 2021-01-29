from funcx.serialize import FuncXSerializer
import numpy as np


def double(x, y=3):
    return x * y


def test():
    fn_buf = fxs.serialize(double)
    args = fxs.serialize([5])
    kwargs = fxs.serialize({})
    payload = fn_buf + args + kwargs
    print(payload)


def test_pack_unpack(fxs):
    fn_buf = fxs.serialize(double)
    args = fxs.serialize([5])
    kwargs = fxs.serialize({})
    print("All items serialized")
    payload = fxs.pack_buffers([fn_buf, args, kwargs])
    print("Payload : \n", payload)

    unpacked = fxs.unpack_buffers(payload)
    print("Unpacked : ", unpacked)
    s_fn, s_args, s_kwargs = unpacked
    fn = fxs.deserialize(s_fn)
    f_args = fxs.deserialize(s_args)
    f_kwargs = fxs.deserialize(s_kwargs)

    print("Fn, args, and kwargs: ", fn, f_args, f_kwargs)

    result = fn(*f_args, **f_kwargs)

    print("Result: ", result)


def test_pack_unpack_2(fxs):
    fn_buf = fxs.serialize(double)
    args = fxs.serialize([5])
    kwargs = fxs.serialize({})
    print("All items serialized")
    payload = fxs.pack_buffers([fn_buf, args, kwargs])
    print("Payload : \n", payload)

    fn, f_args, f_kwargs = fxs.unpack_and_deserialize(payload)

    print("Fn, args, and kwargs: ", fn, f_args, f_kwargs)

    result = fn(*f_args, **f_kwargs)

    print("Result: ", result)


def numpy_sum(array):
    # import numpy as np
    return array.sum()


def test_numpy_arrays(fxs):

    fn_buf = fxs.serialize(numpy_sum)
    args = fxs.serialize([np.array([1, 2, 3, 4, 5])])
    kwargs = fxs.serialize({})
    print("All items serialized")
    payload = fxs.pack_buffers([fn_buf, args, kwargs])
    print("Payload : \n", payload)

    fn, f_args, f_kwargs = fxs.unpack_and_deserialize(payload)

    print("Fn, args, and kwargs: ", fn, f_args, f_kwargs)

    result = fn(*f_args, **f_kwargs)

    print("Result: ", result)


def test_pack_unpack_3(fxs):
    fn_buf = fxs.serialize(double)
    args = fxs.serialize([5])
    kwargs = fxs.serialize({})
    print("All items serialized")
    payload = fxs.pack_buffers([fn_buf, args, kwargs])
    print("Payload : \n", payload)

    fn, f_args, f_kwargs = fxs.unpack_and_deserialize(payload)

    print("Fn, args, and kwargs: ", fn, f_args, f_kwargs)

    result = fn(*f_args, **f_kwargs)

    print("Result: ", result)


if __name__ == "__main__":

    fxs = FuncXSerializer()

    # test_pack_unpack(fxs)
    # test_pack_unpack_2(fxs)
    # test_numpy_arrays(fxs)
    test_pack_unpack_3(fxs)
