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

tSize = c_int32
hdfsFS = c_void_p

class tObjectKind(Structure):
  _fields_ = [('kObjectKindFile', c_char),
              ('kObjectKindDirectory', c_char)]

# hdfsStreamType
UNINITIALIZED = 0
INPUT = 1
OUTPUT = 2

class hdfsFile(Structure):
  _fields_ = [('file', c_void_p),
              ('type', c_int)]

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
def hdfsConnect(hostname, port):
  """Returns an HDFS filesystem connection"""
  fs = libhdfs.hdfsConnect(hostname, port)
  if not fs:
    raise HdfsError('Failed connecting to %s:%d' % (hostname, port))
  return fs
  
# int hdfsDisconnect(hdfsFS fs);
libhdfs.hdfsDisconnect.argtypes = [c_void_p]
def hdfsDisconnect(fs):
  if libhdfs.hdfsDisconnect(fs):
    raise HdfsError('Failed disconnecting!')

# int hdfsExists(hdfsFS fs, const char *path);
libhdfs.hdfsExists.argtypes = [c_void_p, c_char_p]
def hdfsExists(fs, path):
  """Returns True if the path exists"""
  if libhdfs.hdfsExists(fs, path) == 0:
    return True
  else:
    return False

# hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries);
libhdfs.hdfsListDirectory.argtypes = [c_void_p, c_char_p, POINTER(c_int)]
libhdfs.hdfsListDirectory.restype = POINTER(hdfsFileInfo)
def hdfsListDirectory(fs, path):
  """Returns a list of absolute path names"""
  if not hdfsExists(fs, path):
    return None

  path = c_char_p(path)
  num_entries = c_int()
  entries = []
  entries_p = libhdfs.hdfsListDirectory(fs, path, pointer(num_entries))
  [entries.append(entries_p[i].mName) for i in range(num_entries.value)]
  return sorted(entries)

# hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
#                       int bufferSize, short replication, tSize blocksize);
libhdfs.hdfsOpenFile.argtypes = [c_void_p, c_char_p, c_int, c_int, c_short, c_int32]
#libhdfs.hdfsOpenFile.restype = hdfsFile
libhdfs.hdfsOpenFile.restype = c_void_p
def hdfsOpen(fs, filename, mode, buffer_size=0, replication=0, block_size=0):
  if not fs:
    raise HdfsError('No filesystem!')

  flags = None
  if mode == 'r':
    flags = os.O_RDONLY
  elif mode == 'w':
    flags = os.O_WRONLY
  else:
    raise HdfsError('Invalid open flags.')

  fh = libhdfs.hdfsOpenFile(fs, filename, flags, buffer_size,
                            replication, block_size)
  if not fh:
    raise HdfsError('Failed opening <%s>' % filename)

  return fh

# int hdfsCloseFile(hdfsFS fs, hdfsFile file);
libhdfs.hdfsCloseFile.argtypes = [c_void_p, c_void_p]
def hdfsClose(fs, fh):
  if (libhdfs.hdfsCloseFile(fs, fh) == -1):
    raise HdfsError()

#tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length);
libhdfs.hdfsWrite.argtypes = [hdfsFS, c_void_p, c_void_p, tSize]
libhdfs.hdfsWrite.restype = tSize
def hdfsWrite(fs, fh, buffer):
  sb = create_string_buffer(buffer)
  buffer_p = cast(sb, c_void_p)

  ret = libhdfs.hdfsWrite(fs, fh, buffer_p, len(buffer))

  if ret == -1:
    raise HdfsError('write failure')
  return ret

# tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);
libhdfs.hdfsRead.argtypes = [hdfsFS, c_void_p, c_void_p, tSize]
libhdfs.hdfsRead.restype = tSize
def hdfsRead(fs, fh):
  if not fs:
    raise HdfsError('No filesystem!')
  if not fh:
    raise HdfsError('No file handle!')

  buffer = create_string_buffer(1024*1024)
  buffer_p = cast(buffer, c_void_p)

  ret = libhdfs.hdfsRead(fs, fh, buffer_p, 1024*1024)
  if ret == -1:
    raise HdfsError('read failure')
  return buffer.value[0:ret]

# EOF
