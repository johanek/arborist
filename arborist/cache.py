from redis import Redis
import logging
from datetime import datetime
import pickle
LOGGER = logging.getLogger('arborist')

class StreamCache(object):

    def __init__(self):
        self.redis = Redis()

    def write_to_cache(self, messages, start_time):
        self.redis.set(
            int(start_time.timestamp()), pickle.dumps(messages), ex=300)
        LOGGER.info("wrote {} messages to redis for time bucket {}".format(
            len(messages), start_time.isoformat()))

    def read_from_cache(self, name, window):
        LOGGER.info("called read_from_cache {} {}".format(name, window))
        start_time = int(datetime.now().timestamp()) - window - 10
        messages = []
        for key in self.redis.keys():
            if int(key) > start_time:
                data = self.redis.get(key)
                messages = messages + pickle.loads(data)
        return messages