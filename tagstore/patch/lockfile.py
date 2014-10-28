import os
from logging import getLogger, CRITICAL

from zc.lockfile import LockFile, LockError


getLogger('zc.lockfile').setLevel(CRITICAL)


class RLockFile(object):
    def __init__(self, name):
        self.name = name
        self.pid = None

    def acquire(self):
        """Spin until locked.

        Acquire is reentrant.

        """
        while True:
            try:
                self.lock = LockFile(self.name)
            except LockError as error:
                pid = os.getpid()
                lockpid = int(open(self.name, 'r').read().strip())
                # If locked and pid is the same, this is reentrant.
                if lockpid == pid:
                    break
            else:
                break

    def release(self):
        self.lock.close()
