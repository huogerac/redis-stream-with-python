from os import environ
from redis import Redis
from uuid import uuid1
from time import sleep

stream_key = environ.get("STREAM", "jarless-1")


def connect_to_redis():
    hostname = environ.get("REDIS_HOSTNAME", "localhost")
    port = environ.get("REDIS_PORT", 6379)

    r = Redis(hostname, port, retry_on_timeout=True)
    return r


def get_data(redis_connection):
    last_id = 0
    sleep_ms = 5000
    while True:
        try:
            resp = redis_connection.xread(
                {stream_key: last_id}, count=1, block=sleep_ms
            )
            if resp:
                key, messages = resp[0]
                last_id, data = messages[0]
                print(last_id)
                print(data)

        except ConnectionError as e:
            print("ERROR REDIS CONNECTION: {}".format(e))


if __name__ == "__main__":
    connection = connect_to_redis()
    get_data(connection)
