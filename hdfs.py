#!/usr/bin/python26

import logging
import os

from ctypes import *

logger = logging.getLogger()


class HdfsError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)


if not os.getenv("CLASSPATH"):
  raise HdfsError('Failed loading libhdfs.so because CLASSPATH environment variable is not set.')


libhdfs = cdll.LoadLibrary('libhdfs.so')


class tObjectKind(Structure):
  _fields_ = [('kObjectKindFile', c_char),
              ('kObjectKindDirectory', c_char)]


# TODO: Figure out the "right way" to deal with typedef. This feels fragile.
# TODO: time_t is almost certainly incorrect.
class hdfsFileInfo(Structure):
  _fields_ = [('mKind', tObjectKind),      # file or directory
              ('mName', c_char_p),         # the name of the file
              ('mLastMod', c_long),         # the last modification time for the file in seconds
              ('mSize', c_longlong),       # the size of the file in bytes
              ('mReplication', c_short),   # the count of replicas
              ('mBlockSize', c_longlong),  # the block size for the file
              ('mOwner', c_char_p),        # the owner of the file
              ('mGroup', c_char_p),        # the group associated with the file
              ('mPermissions', c_short),   # the permissions associated with the file
              ('mLastAccess', c_long)]      # the last access time for the file in seconds


# hdfsFS hdfsConnect(const char* host, tPort port);
libhdfs.hdfsConnect.argtypes = [c_char_p, c_uint16]
libhdfs.hdfsConnect.restype = c_void_p

# int hdfsDisconnect(hdfsFS fs);
libhdfs.hdfsDisconnect.argtypes = [c_void_p]

# hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries);
libhdfs.hdfsListDirectory.argtypes = [c_void_p, c_char_p, POINTER(c_int)]
libhdfs.hdfsListDirectory.restype = POINTER(hdfsFileInfo)


class Hdfs(object):
  # TODO: Maybe have a mode that reuses connections instead of one per file?

  O_RDONLY = 1
  O_WRONLY = 2

  def __init__(self, hostname, port):
    self.fs = None
    self.fh = None
    self.filename = None
    self.hostname = hostname
    self.port = port
    logger.critical('Connecting to <%s:%d>' % (self.hostname, self.port))
    self.fs = libhdfs.hdfsConnect(self.hostname, self.port)
    if not self.fs:
      raise HdfsError('Failed connecting to %s:%d' % (self.hostname, self.port))

  def __del__(self):
    if self.fh:
      self.close()
    if self.fs:
      logger.critical('Disconnecting from %s:%d' % (self.hostname, self.port))
      libhdfs.hdfsDisconnect(self.fs)

  def open(self, filename, mode, buffer_size=0, replication=0, block_size=0):
    if not self.fs:
      raise HdfsError()

    flags = None
    if mode == 'r':
      flags = Hdfs.O_RDONLY
    elif mode == 'w':
      flags = Hdfs.O_WRONLY
    else:
      raise HdfsError('Invalid open flags.')

    logger.critical('Opening <%s>' % filename)
    print self.fs, filename, flags, buffer_size, replication, block_size

    self.filename = filename
    self.fh = libhdfs.hdfsOpenFile(self.fs, self.filename, flags, buffer_size, replication, block_size)
    if not self.fh:
      raise HdfsError('Failed opning file <%s> on filesystem <%s:%d>' % (self.filename, self.hostname, self.port))

  def close(self):
    if not self.fs:
      raise HdfsError('No filesystem!')

    if not self.fh:
      raise HdfsError('No file handle!')

    logger.critical('Closing file <%s> on filesystem <%s:%d>' % (self.filename, self.hostname, self.port))
    if (libhdfs.hdfsCloseFile(self.fs, self.fh) == -1):
      raise HdfsError('Failed closing file <%s> on filesystem <%s:%d>' % (self.filename, self.hostname, self.port))


  def ls(self, path):
    path = c_char_p(path)
    num_entries = c_int()

    entries = libhdfs.hdfsListDirectory(self.fs, path, pointer(num_entries))
    logger.critical('Number of entries: %d' % num_entries.value)
    for i in range(num_entries.value):
      print entries[i].mName


if __name__ == '__main__':
  logging.basicConfig()
  hdfs = Hdfs('hadoop.twitter.com', 8020)
  hdfs.ls('/user/travis')
  #hdfs.open('/user/travis/hosts', 'r')
  

# EOF
