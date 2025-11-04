import time

from elasticsearch import Elasticsearch

if __name__ == '__main__':
    es_client = Elasticsearch(hosts='http://elasticsearch:9200', verify_certs=False, ssl_show_warn=False)

    while True:
        if es_client.ping():
            break
        time.sleep(1)