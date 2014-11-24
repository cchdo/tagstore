import os
import os.path
from logging import getLogger, CRITICAL
from time import sleep

from tagstore.vendor.lockfile import LockFile, LockError


def lockpath(basepath, name):
    return os.path.abspath(os.path.join(basepath, '.lock-{0}'.format(name)))


class RLockFile(object):
    def __init__(self, name):
        self.name = name
        self.pidfile = self.name + '.pid'

    def acquire(self):
        """Spin until locked.

        Acquire is reentrant.

        """
        pid = getnode() + str(os.getpid())
        while True:
            try:
                self.lock = LockFile(self.name)
            except LockError as error:
                try:
                    lockpid = open(self.pidfile, 'r').read()
                except (IOError, OSError, ValueError):
                    pass
                # If locked and pid is the same, this is reentrant.
                if lockpid == pid:
                    break
            else:
                with open(self.pidfile, 'w') as fff:
                    fff.write(pid)
                break
            sleep(0.5)

    def release(self):
        try:
            self.lock.close()
        except AttributeError:
            pass
