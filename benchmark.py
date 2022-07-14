from contextlib import contextmanager
import time

from gevent import monkey
from nanodis.client import Client


monkey.patch_all()


@contextmanager
def timed(s):
    start = time.time()
    yield

    duration = round(time.time() - start, 3)
    print(f'{(s, duration)}')


def run_benchmark(client):
    n = 10000

    with timed('get/set'):
        for i in range(n):
            client.set(f'k {i}, v {i}')

        for i in range(n + int(n * 0.1)):
            client.get(f'k {i}')

    with timed('serializing lists'):
        some_list = [1, 2, 3, 4, 5, 6, [7, 8, 9, [10, 11, 12], 13], 14, 15]

        for i in range(n):
            client.set(f'k {i, some_list}')

        for i in range(n):
            client.get(f'k {i}')

    with timed('serializing dicts'):
        some_dict = {'k1': 'v1', 'k2': 'v2', 'k3': {'v3': {'v4': 'v5'}}}

        for i in range(n):
            client.set(f'k {i, some_dict}')

        for i in range(n):
            client.get(f'k {i}')


def main():
    client = Client()

    try:
        run_benchmark(client)
    finally:
        client.close()


if __name__ == '__main__':
    main()
