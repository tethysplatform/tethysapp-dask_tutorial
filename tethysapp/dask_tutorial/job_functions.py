import time
import dask


def inc(x):
    time.sleep(30)
    return x + 1


def double(x):
    time.sleep(30)
    return x + 2


def add(x, y):
    time.sleep(30)
    return x + y


def total():
    output = []
    for x in range(3):
        a = dask.delayed(inc, pure=False)(x)
        b = dask.delayed(double, pure=False)(x)
        c = dask.delayed(add, pure=False)(a, b)
        output.append(c)
    return dask.delayed(sum, pure=False)(output)
