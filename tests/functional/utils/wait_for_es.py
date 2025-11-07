import backoff
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

if __name__ == '__main__':
    @backoff.on_exception(
        backoff.expo,
        ConnectionError,
        max_time=300,
        max_tries=50
    )
    def wait_for_es():
        es_client = Elasticsearch(
            hosts='http://elasticsearch:9200',
            request_timeout=5
        )
        if not es_client.ping():
            raise ConnectionError("Elasticsearch ping failed")
        print("✅ Elasticsearch is ready!")

    wait_for_es()