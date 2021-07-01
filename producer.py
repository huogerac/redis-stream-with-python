from os import environ
from redis import Redis
from uuid import uuid1
from time import sleep

stream_key = environ.get("STREAM", "jarless-1")
producer = environ.get("PRODUCER", "user-1")


def connect_to_redis():
    hostname = environ.get("REDIS_HOSTNAME", "localhost")
    port = environ.get("REDIS_PORT", 6379)

    r = Redis(hostname, port, retry_on_timeout=True)
    return r


def send_data(redis_connection, max_messages=10):
    count = 0
    while count < max_messages:
        uid = uuid1()
        try:
            data = {
                "producer": producer,
                "some_id": str(uid),
                "count": count,
            }
            resp = redis_connection.xadd(stream_key, data)
            print(resp)
            count += 1

        except ConnectionError as e:
            print("ERROR REDIS CONNECTION: {}".format(e))

        sleep(1)


if __name__ == "__main__":
    MAX_MESSAGES = 2
    connection = connect_to_redis()
    send_data(connection, MAX_MESSAGES)
