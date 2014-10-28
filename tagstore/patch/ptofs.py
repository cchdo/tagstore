"""Patch ofs.local.storedjson.PersistentState to be more threadsafe

* Prevent concurrent access to persistence file
* Prevent concurrent access to PersistentState by PTOFS (This will also prevent
concurrent read and write to PTOFS)

"""

import os
import os.path
from ofs.local import PTOFS
from ofs.local.storedjson import PersistentState, PERSISTENCE_FILENAME
from logging import getLogger, DEBUG, WARN

from lockfile import RLockFile


log = getLogger(__name__)
log.setLevel(WARN)


persistence_lock = RLockFile('.lock-persistence')
ptofs_lock = RLockFile('.lock-ptofs')


old_revert = PersistentState.revert 
def new_revert(self):
    log.debug('lock persist acquiring {0}'.format(os.getpid()))
    persistence_lock.acquire()
    log.debug('lock persist acquired {0}'.format(os.getpid()))
    try:
        old_revert(self)
    finally:
        persistence_lock.release()
    log.debug('lock persist released {0}'.format(os.getpid()))
PersistentState.revert = new_revert


old_sync = PersistentState.sync
def new_sync(self):
    log.debug('lock persist acquiring {0}'.format(os.getpid()))
    persistence_lock.acquire()
    log.debug('lock persist acquired {0}'.format(os.getpid()))
    try:
        old_sync(self)
    finally:
        persistence_lock.release()
        log.debug('lock persist released {0}'.format(os.getpid()))

PersistentState.sync = new_sync


old_init = PersistentState.__init__
def new_init(self, filepath=None, filename=PERSISTENCE_FILENAME, create=True):
    log.debug('lock PTOFS acquiring {0}'.format(os.getpid()))
    ptofs_lock.acquire()
    log.debug('lock PTOFS acquired {0}'.format(os.getpid()))
    old_init(self, filepath, filename, create)
PersistentState.__init__ = new_init


try:
    old_del = PersistentState.__del__
except AttributeError:
    old_del = lambda x: None
def new_del(self):
    old_del(self)
    # Release PTOFS lock whenever persisted state is collected.
    try:
        ptofs_lock.release()
        log.debug('lock PTOFS released {0}'.format(os.getpid()))
    except RuntimeError:
        log.error('lock PTOFS failed to release {0}'.format(os.getpid()))
PersistentState.__del__ = new_del
