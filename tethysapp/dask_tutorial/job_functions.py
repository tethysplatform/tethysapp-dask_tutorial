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
    time.sleep(3000)
    return sum(x)


# Delayed
def total():
    output = []
    for x in range(3):
        a = dask.delayed(inc, pure=False)(x)
        b = dask.delayed(double, pure=False)(x)
        c = dask.delayed(add, pure=False)(a, b)
        output.append(c)
    return dask.delayed(sum_up, pure=False)(output)


# Future
def total_future(client):
    output = []
    for x in range(3):
        a = client.submit(inc, x)
        b = client.submit(double, x)
        c = client.submit(add, a, b)
        output.append(c)
    return client.submit(sum_up, output)


# Multiple Future Instances
def multiple_future(client):
    output = []
    for x in range(3):
        a = client.submit(inc, x)
        b = client.submit(double, x)
        c = client.submit(add, a, b)
        output.append(c)
    return output


def convert_to_dollar_sign(result):
    return '$' + str(result)