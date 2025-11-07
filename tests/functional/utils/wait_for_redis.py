import backoff
import logging
from redis import Redis
from redis.exceptions import ConnectionError

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    @backoff.on_exception(
        backoff.expo,
        ConnectionError,
        max_time=300,
        max_tries=50
    )
    def wait_for_redis():
        redis_client = Redis(
            host='redis',
            port=6379,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5
        )
        if not redis_client.ping():
            raise ConnectionError("Redis ping failed")
        logger.info("✅ Redis is ready!")

    wait_for_redis()