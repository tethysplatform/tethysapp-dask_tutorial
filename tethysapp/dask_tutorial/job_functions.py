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

# Delayed Job
def delayed_job():
    output = []
    for x in range(3):
        a = dask.delayed(inc, pure=False)(x)
        b = dask.delayed(double, pure=False)(x)
        c = dask.delayed(add, pure=False)(a, b)
        output.append(c)
    return dask.delayed(sum_up, pure=False)(output)

# Distributed Job
def distributed_job(client):
    output = []
    for x in range(3):
        a = client.submit(inc, x, pure=False)
        b = client.submit(double, x, pure=False)
        c = client.submit(add, a, b, pure=False)
        output.append(c)
    return client.submit(sum_up, output)

# Multiple Leaf Distributed Job
def multiple_leaf_job(client):
    output = []
    for x in range(3):
        a = client.submit(inc, x, pure=False)
        b = client.submit(double, x, pure=False)
        c = client.submit(add, a, b, pure=False)
        output.append(c)
    return output