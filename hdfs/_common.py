import os
from ctypes import *

tSize = c_int32
tTime = c_long
tOffset = c_int64
tPort = c_uint16
hdfsFS = c_void_p
hdfsFile = c_void_p

class HdfsError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

if not os.getenv("CLASSPATH"):
  raise HdfsError('Failed loading libhdfs.so because CLASSPATH environment variable is not set.')

class tObjectKind(Structure):
  _fields_ = [('kObjectKindFile', c_char),
              ('kObjectKindDirectory', c_char)]

class hdfsFileInfo(Structure):
  _fields_ = [('mKind', tObjectKind),      # file or directory
              ('mName', c_char_p),         # the name of the file
              ('mLastMod', tTime),         # the last modification time for the file in seconds
              ('mSize', c_longlong),       # the size of the file in bytes
              ('mReplication', c_short),   # the count of replicas
              ('mBlockSize', c_longlong),  # the block size for the file
              ('mOwner', c_char_p),        # the owner of the file
              ('mGroup', c_char_p),        # the group associated with the file
              ('mPermissions', c_short),   # the permissions associated with the file
              ('mLastAccess', tTime)]      # the last access time for the file in seconds

libhdfs = cdll.LoadLibrary('libhdfs.so')
libhdfs.hdfsAvailable.argtypes = [hdfsFS, hdfsFile]
libhdfs.hdfsChmod.argtypes = [hdfsFS, c_char_p, c_short]
libhdfs.hdfsChown.argtypes = [hdfsFS, c_char_p, c_char_p, c_char_p]
libhdfs.hdfsCloseFile.argtypes = [hdfsFS, hdfsFile]
libhdfs.hdfsConnect.argtypes = [c_char_p, tPort]
libhdfs.hdfsConnect.restype = hdfsFS
libhdfs.hdfsCopy.argtypes = [hdfsFS, c_char_p, hdfsFS, c_char_p]
libhdfs.hdfsCreateDirectory.argtypes = [hdfsFS, c_char_p]
libhdfs.hdfsDelete.argtypes = [hdfsFS, c_char_p]
libhdfs.hdfsDisconnect.argtypes = [hdfsFS]
libhdfs.hdfsExists.argtypes = [hdfsFS, c_char_p]
libhdfs.hdfsFlush.argtypes = [hdfsFS, hdfsFile]
libhdfs.hdfsGetCapacity.argtypes = [hdfsFS]
libhdfs.hdfsGetCapacity.restype = tOffset
libhdfs.hdfsGetDefaultBlockSize.argtypes = [hdfsFS]
libhdfs.hdfsGetDefaultBlockSize.restype = tOffset
libhdfs.hdfsGetPathInfo.argtypes = [hdfsFS, c_char_p]
libhdfs.hdfsGetPathInfo.restype = POINTER(hdfsFileInfo)
libhdfs.hdfsGetUsed.argtypes = [hdfsFS]
libhdfs.hdfsGetUsed.restype = tOffset
libhdfs.hdfsListDirectory.argtypes = [hdfsFS, c_char_p, POINTER(c_int)]
libhdfs.hdfsListDirectory.restype = POINTER(hdfsFileInfo)
libhdfs.hdfsMove.argtypes = [hdfsFS, c_char_p, hdfsFS, c_char_p]
libhdfs.hdfsOpenFile.argtypes = [hdfsFS, c_char_p, c_int, c_int, c_short, tSize]
libhdfs.hdfsOpenFile.restype = hdfsFile
libhdfs.hdfsPread.argtypes = [hdfsFS, hdfsFile, tOffset, c_void_p, tSize]
libhdfs.hdfsPread.restype = tSize
libhdfs.hdfsRead.argtypes = [hdfsFS, hdfsFile, c_void_p, tSize]
libhdfs.hdfsRead.restype = tSize
libhdfs.hdfsRename.argtypes = [hdfsFS, c_char_p, c_char_p]
libhdfs.hdfsSeek.argtypes = [hdfsFS, hdfsFile, tOffset]
libhdfs.hdfsSetReplication.argtypes = [hdfsFS, c_char_p, c_int16]
libhdfs.hdfsTell.argtypes = [hdfsFS, hdfsFile]
libhdfs.hdfsTell.restype = tOffset
libhdfs.hdfsUtime.argtypes = [hdfsFS, c_char_p, tTime, tTime]
libhdfs.hdfsWrite.argtypes = [hdfsFS, hdfsFile, c_void_p, tSize]
libhdfs.hdfsWrite.restype = tSize
libhdfs.hdfsGetHosts.restype = POINTER(POINTER(c_char_p))
libhdfs.hdfsGetHosts.argtypes = [hdfsFS, c_char_p, tOffset, tOffset]
