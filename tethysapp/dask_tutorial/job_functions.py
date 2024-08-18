import time
import dask


def inc(x):
    time.sleep(3)
    return x + 1


def double(x):
    time.sleep(3)
    return x + 2


def add(x, y):
    time.sleep(10)
    return x + y


def sum_up(x):
    time.sleep(5)
    return sum(x)


def convert_to_dollar_sign(result):
    return '$' + str(result)