import time
import dask


def inc(x):
    time.sleep(2)
    return x + 1


def double(x):
    time.sleep(2)
    return x + 2


def add(x, y):
    time.sleep(4)
    return x + y


def sum(output):
    time.sleep(60)
    return output


def total():
    output = []
    for x in range(3):
        a = dask.delayed(inc)(x)
        b = dask.delayed(double)(x)
        c = dask.delayed(add)(a, b)
        output.append(c)
    return dask.delayed(sum)(output)
