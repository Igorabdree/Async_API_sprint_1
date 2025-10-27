import time

from redis import Redis

if __name__ == '__main__':
    es_client = Redis(host='127.0.0.1', port = 6379, db=0, decode_responses=True, socket_connect_timeout=5)
    while True:
        if es_client.ping():
            break
        time.sleep(1)
