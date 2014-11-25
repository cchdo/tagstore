import os
import os.path
from logging import getLogger, CRITICAL
from time import sleep
from uuid import getnode

from tagstore.vendor.lockfile import (
    LockFile, LockError, NotLockedError, TimeOutError, AlreadyLockedError
)


log = getLogger(__name__)


def lockpath(basepath, name):
    return os.path.abspath(os.path.join(basepath, '.lock-{0}'.format(name)))


class RLockFile(object):
    def __init__(self, name):
        self.name = name
        self.lock = LockFile(self.name)
        self.pidfile = self.name + '.pid'
        self.locks = 0

    def acquire(self):
        """Spin until locked.

        Acquire is reentrant.

        """
        pid = '{0}_{1}'.format(getnode(), os.getpid())
        while True:
            log.debug('acquiring {0} {1}'.format(self.name, pid))
            try:
                self.lock.lock(timeout=1e-6)
            except (TimeOutError, AlreadyLockedError) as error:
                try:
                    lockpid = open(self.pidfile, 'r').read()
                except (IOError, OSError, ValueError):
                    lockpid = None
                # If locked and pid is the same, this is reentrant.
                log.debug('{0} {1}'.format(lockpid, pid))
                if lockpid == pid:
                    log.debug(u'reentrant acquisition')
                    self.locks += 1
                    break
            else:
                with open(self.pidfile, 'w') as fff:
                    fff.write(pid)
                break
            sleep(0.5)
        log.debug('acquired {0} {1}'.format(self.name, pid))

    def release(self):
        #if self.locks != 0:
        #    self.locks -= 1
        #    log.debug('released reentrance {0}'.format(self.name))
        #    return
        try:
            self.lock.unlock()
        except (AttributeError, NotLockedError):
            pass
        log.debug('released {0}'.format(self.name))
