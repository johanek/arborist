from time import time
import logging
import itertools
from threading import RLock
LOGGER = logging.getLogger('arborist')

class CacheEntry():
    def __init__(self, value, ttl=20, expires_at=None):
        self.value = value
        if expires_at:
            self.expires_at = expires_at
        else:
            self.expires_at = time() + ttl
        self.created_at = time()
        self._expired = False

    def expired(self, expire_clock=None):
        if not expire_clock:
            expire_clock = time()

        if self._expired is False:
            return (self.expires_at < expire_clock)
        else:
            return self._expired
    
    def older_than(self, ttl, expire_clock=None):
        if not expire_clock:
            expire_clock = time()

        threshold = expire_clock - ttl
        return self.created_at < threshold


class CacheList():
    def __init__(self):
        self.entries = []
        self.lock = RLock()
        self.oldest_offset = 0

    def add_entry(self, value, ttl=20, expires_at=None):
        with self.lock:
            self.entries.append(CacheEntry(value, ttl, expires_at))

    def read_entries(self, expire_clock=None):
        with self.lock:
            expiring_offsets = [x.value['_offset'] for x in self.entries if x.expired()]
            if expiring_offsets:
                self.oldest_offset = max(expiring_offsets)
                LOGGER.info(f"Expiring offsets to {self.oldest_offset}")
            self.entries = list(itertools.dropwhile(lambda x: x.expired(expire_clock), self.entries))
            return self.entries
            # return [entry.value for entry in self.entries]

    def read_entries_maxage(self, maxage):
        entries = list(itertools.dropwhile(lambda x: x.older_than(maxage), self.read_entries()))
        return [entry.value for entry in entries]