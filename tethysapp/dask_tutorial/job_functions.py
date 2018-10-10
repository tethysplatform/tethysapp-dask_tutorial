import time
import dask


def inc(x):
    time.sleep(2)
    return x + 1


def double(x):
    time.sleep(3)
    return x + 2


def add(x, y):
    time.sleep(1)
    return x + y


def sum_up(x):
    time.sleep(5)
    return sum(x)


def total():
    output = []
    for x in range(3):
        a = dask.delayed(inc, pure=False)(x)
        b = dask.delayed(double, pure=False)(x)
        c = dask.delayed(add, pure=False)(a, b)
        output.append(c)
    return dask.delayed(sum_up, pure=False)(output)
