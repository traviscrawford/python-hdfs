#!/usr/bin/python26

"""
This wrapper works with:

c0e06197395055711483fa1c747bd8cd  /usr/include/hdfs.h
"""

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
tTime = c_long # double-check this
tOffset = c_int64
tPort = c_uint16
hdfsFS = c_void_p


class tObjectKind(Structure):
  _fields_ = [('kObjectKindFile', c_char),
              ('kObjectKindDirectory', c_char)]


# hdfsStreamType
UNINITIALIZED = 0
INPUT = 1
OUTPUT = 2

hdfsFile = c_void_p
#class hdfsFile(Structure):
#  _fields_ = [('file', c_void_p),
#              ('type', c_int)]

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

# hdfsFS hdfsConnectAsUser(const char* host, tPort port, const char *user , const char *groups[], int groups_size );
libhdfs.hdfsConnectAsUser.argtypes = [c_char_p, tPort, c_char_p, c_char_p, c_int]
libhdfs.hdfsConnectAsUser.restype = hdfsFS
def hdfsConnectAsUser(host, port, user, groups, groups_size):
  """Connect to a hdfs file system as a specific user

  @param host A string containing either a host name, or an ip address
  of the namenode of a hdfs cluster. 'host' should be passed as NULL if
  you want to connect to local filesystem. 'host' should be passed as
  'default' (and port as 0) to used the 'configured' filesystem
  (core-site/core-default.xml).
  @param port The port on which the server is listening.
  @param user the user name (this is hadoop domain user). Or NULL is equivelant to hhdfsConnect(host, port)
  @param groups the groups (these are hadoop domain groups)
  @return Returns a handle to the filesystem or NULL on error.
  """
  fs = libhdfs.hdfsConnectAsUser(host, port, user, )
  if not fs:
    raise HdfsError('Failed connecting to %s:%d' % (hostname, port))
  return fs

# hdfsFS hdfsConnect(const char* host, tPort port);
libhdfs.hdfsConnect.argtypes = [c_char_p, c_uint16]
libhdfs.hdfsConnect.restype = hdfsFS
def hdfsConnect(hostname, port):
  """Connect to a hdfs file system.

  @param host A string containing either a host name, or an ip address
  of the namenode of a hdfs cluster. 'host' should be passed as NULL if
  you want to connect to local filesystem. 'host' should be passed as
  'default' (and port as 0) to used the 'configured' filesystem
  (core-site/core-default.xml).
  @param port The port on which the server is listening.
  @return Returns a handle to the filesystem or NULL on error.
  """
  fs = libhdfs.hdfsConnect(hostname, port)
  if not fs:
    raise HdfsError('Failed connecting to %s:%d' % (hostname, port))
  return fs

"""
/**
* This are the same as hdfsConnectAsUser except that every invocation returns a new FileSystem handle.
* Applications should call a hdfsDisconnect for every call to hdfsConnectAsUserNewInstance.
*/
hdfsFS hdfsConnectAsUserNewInstance(const char* host, tPort port, const char *user , const char *groups[], int groups_size );
hdfsFS hdfsConnectNewInstance(const char* host, tPort port);
hdfsFS hdfsConnectPath(const char* uri);
"""

# int hdfsDisconnect(hdfsFS fs);
libhdfs.hdfsDisconnect.argtypes = [c_void_p]
def hdfsDisconnect(fs):
  """Disconnect from the hdfs file system.

  @param fs The configured filesystem handle.
  @return Returns 0 on success, -1 on error.
  """
  if libhdfs.hdfsDisconnect(fs):
    raise HdfsError('Failed disconnecting!')

# hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
#                       int bufferSize, short replication, tSize blocksize);
libhdfs.hdfsOpenFile.argtypes = [c_void_p, c_char_p, c_int, c_int, c_short, c_int32]
libhdfs.hdfsOpenFile.restype = hdfsFile
def hdfsOpen(fs, filename, mode, buffer_size=0, replication=0, block_size=0):
  """Open a hdfs file in given mode.

  @param fs The configured filesystem handle.
  @param path The full path to the file.
  @param flags - an | of bits/fcntl.h file flags - supported flags are
  O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies
  O_TRUNCAT), O_WRONLY|O_APPEND. Other flags are generally ignored other
  than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno
  equal ENOTSUP.
  @param bufferSize Size of buffer for read/write - pass 0 if you want
  to use the default configured values.
  @param replication Block replication - pass 0 if you want to use
  the default configured values.
  @param blocksize Size of block - pass 0 if you want to use the
  default configured values.
  @return Returns the handle to the open file or NULL on error.
  """
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
  """Close an open file.

  @param fs The configured filesystem handle.
  @param file The file handle.
  @return Returns 0 on success, -1 on error.
  """
  if (libhdfs.hdfsCloseFile(fs, fh) == -1):
    raise HdfsError()

# int hdfsExists(hdfsFS fs, const char *path);
libhdfs.hdfsExists.argtypes = [c_void_p, c_char_p]
libhdfs.hdfsExists.restype = c_int
def hdfsExists(fs, path):
  """Checks if a given path exsits on the filesystem

  @param fs The configured filesystem handle.
  @param path The path to look for
  @return Returns 0 on success, -1 on error.
  """
  if libhdfs.hdfsExists(fs, path) == 0:
    return True
  else:
    return False

# int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos);
libhdfs.hdfsSeek.argtypes = [hdfsFS, hdfsFile, tOffset]
libhdfs.hdfsSeek.restype = c_int
def hdfsSeek(fs, fh, desired_pos):
  """Seek to given offset in file. This works only for
  files opened in read-only mode.

  @param fs The configured filesystem handle.
  @param file The file handle.
  @param desiredPos Offset into the file to seek into.
  @return Returns 0 on success, -1 on error.
  """
  if libhdfs.hdfsSeek(fs, fh, desired_pos) == 0:
    return True
  else:
    return False

# tOffset hdfsTell(hdfsFS fs, hdfsFile file);
libhdfs.hdfsTell.argtypes = [hdfsFS, hdfsFile]
libhdfs.hdfsTell.restype = tOffset
def hdfsTell(fs, fh):
  """Get the current offset in the file, in bytes.

  @param fs The configured filesystem handle.
  @param file The file handle.
  @return Current offset, -1 on error.
  """
  ret = libhdfs.hdfsTell(fs, fh)
  if ret != -1:
    return ret
  else:
    raise HdfsError('hdfsTell failed')

# tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length);
libhdfs.hdfsRead.argtypes = [hdfsFS, c_void_p, c_void_p, tSize]
libhdfs.hdfsRead.restype = tSize
def hdfsRead(fs, fh):
  """Read data from an open file.

  @param fs The configured filesystem handle.
  @param file The file handle.
  @param buffer The buffer to copy read bytes into.
  @param length The length of the buffer.
  @return Returns the number of bytes actually read, possibly less
  than than length;-1 on error."""
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

#/** 
# * hdfsPread - Positional read of data from an open file.
# * @param fs The configured filesystem handle.
# * @param file The file handle.
# * @param position Position from which to read
# * @param buffer The buffer to copy read bytes into.
# * @param length The length of the buffer.
# * @return Returns the number of bytes actually read, possibly less than
# * than length;-1 on error.
# */
#tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
#                void* buffer, tSize length);

#tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer, tSize length);
libhdfs.hdfsWrite.argtypes = [hdfsFS, hdfsFile, c_void_p, tSize]
libhdfs.hdfsWrite.restype = tSize
def hdfsWrite(fs, fh, buffer):
  """Write data into an open file.

  @param fs The configured filesystem handle.
  @param file The file handle.
  @param buffer The data.
  @param length The no. of bytes to write. 
  @return Returns the number of bytes written, -1 on error.
  """
  sb = create_string_buffer(buffer)
  buffer_p = cast(sb, c_void_p)

  ret = libhdfs.hdfsWrite(fs, fh, buffer_p, len(buffer))

  if ret == -1:
    raise HdfsError('write failure')
  return ret


# int hdfsFlush(hdfsFS fs, hdfsFile file);
libhdfs.hdfsFlush.argtypes = [hdfsFS, hdfsFile]
libhdfs.hdfsFlush.restype = c_int
def hdfsFlush(fs, fh):
  """Flush the data.

  * @param fs The configured filesystem handle.
  * @param file The file handle.
  * @return Returns 0 on success, -1 on error.
  """
  raise NotImplementedError, "TODO(travis)"

# int hdfsAvailable(hdfsFS fs, hdfsFile file);
libhdfs.hdfsAvailable.argtypes = [hdfsFS, hdfsFile]
libhdfs.hdfsAvailable.restype = c_int
def hdfsAvailable(fs, fh):
  """Number of bytes that can be read from this input stream without blocking.

  @param fs The configured filesystem handle.
  @param file The file handle.
  @return Returns available bytes; -1 on error.
  """
  raise NotImplementedError, "TODO(travis)"

# int hdfsCopy(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);
libhdfs.hdfsCopy.argtypes = [hdfsFS, c_char_p, hdfsFS, c_char_p]
libhdfs.hdfsCopy.restype = c_int
def hdfsCopy(srcFS, srcPath, dstFS, dstPath):
  """Copy file from one filesystem to another.
  @param srcFS The handle to source filesystem.
  @param src The path of source file.
  @param dstFS The handle to destination filesystem.
  @param dst The path of destination file.
  @return Returns 0 on success, -1 on error.
  """
  raise NotImplementedError, "TODO(travis)"

# int hdfsMove(hdfsFS srcFS, const char* src, hdfsFS dstFS, const char* dst);
libhdfs.hdfsMove.argtypes = [hdfsFS, c_char_p, hdfsFS, c_char_p]
def hdfsMove(srcFS, srcPath, dstFS, dstPath):
  """Move file from one filesystem to another.

  @param srcFS The handle to source filesystem.
  @param src The path of source file.
  @param dstFS The handle to destination filesystem.
  @param dst The path of destination file.
  @return Returns 0 on success, -1 on error.
  """
  raise NotImplementedError, "TODO(travis)"

# int hdfsDelete(hdfsFS fs, const char* path);
libhdfs.hdfsDelete.argtypes = [hdfsFS, c_char_p]
def hdfsDelete(fs, path):
  """Delete file.

  @param fs The configured filesystem handle.
  @param path The path of the file.
  @return Returns 0 on success, -1 on error.
  """
  if (libhdfs.hdfsDelete(fs, path) == -1):
    raise HdfsError('delete failure')

# int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath);
libhdfs.hdfsRename.argtypes = [hdfsFS, c_char_p, c_char_p]
def hdfsRename(fs, oldPath, newPath):
  """Rename file.

  @param fs The configured filesystem handle.
  @param oldPath The path of the source file.
  @param newPath The path of the destination file.
  @return Returns 0 on success, -1 on error.
  """
  if (libhdfs.hdfsRename(fs, oldPath, newPath) == -1):
    raise HdfsError('rename failure')

"""
/** 
* hdfsGetWorkingDirectory - Get the current working directory for
* the given filesystem.
* @param fs The configured filesystem handle.
* @param buffer The user-buffer to copy path of cwd into. 
* @param bufferSize The length of user-buffer.
* @return Returns buffer, NULL on error.
*/
char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize);


/** 
* hdfsSetWorkingDirectory - Set the working directory. All relative
* paths will be resolved relative to it.
* @param fs The configured filesystem handle.
* @param path The path of the new 'cwd'. 
* @return Returns 0 on success, -1 on error. 
*/
int hdfsSetWorkingDirectory(hdfsFS fs, const char* path);


/** 
* hdfsCreateDirectory - Make the given file and all non-existent
* parents into directories.
* @param fs The configured filesystem handle.
* @param path The path of the directory. 
* @return Returns 0 on success, -1 on error. 
*/
int hdfsCreateDirectory(hdfsFS fs, const char* path);


/** 
* hdfsSetReplication - Set the replication of the specified
* file to the supplied value
* @param fs The configured filesystem handle.
* @param path The path of the file. 
* @return Returns 0 on success, -1 on error. 
*/
int hdfsSetReplication(hdfsFS fs, const char* path, int16_t replication);


/** 
* hdfsFileInfo - Information about a file/directory.
*/
typedef struct  {
tObjectKind mKind;   /* file or directory */
char *mName;         /* the name of the file */
tTime mLastMod;      /* the last modification time for the file in seconds */
tOffset mSize;       /* the size of the file in bytes */
short mReplication;    /* the count of replicas */
tOffset mBlockSize;  /* the block size for the file */
char *mOwner;        /* the owner of the file */
char *mGroup;        /* the group associated with the file */
short mPermissions;  /* the permissions associated with the file */
tTime mLastAccess;    /* the last access time for the file in seconds */
} hdfsFileInfo;
"""

# hdfsFileInfo *hdfsListDirectory(hdfsFS fs, const char* path, int *numEntries);
libhdfs.hdfsListDirectory.argtypes = [c_void_p, c_char_p, POINTER(c_int)]
libhdfs.hdfsListDirectory.restype = POINTER(hdfsFileInfo)
def hdfsListDirectory(fs, path):
  """Get list of files/directories for a given directory-path.
  hdfsFreeFileInfo should be called to deallocate memory.

  @param fs The configured filesystem handle.
  @param path The path of the directory. 
  @param numEntries Set to the number of files/directories in path.
  @return Returns a dynamically-allocated array of hdfsFileInfo objects;
  NULL on error.
  """
  if not hdfsExists(fs, path):
    return None

  path = c_char_p(path)
  num_entries = c_int()
  entries = []
  entries_p = libhdfs.hdfsListDirectory(fs, path, pointer(num_entries))
  [entries.append(entries_p[i].mName) for i in range(num_entries.value)]
  return sorted(entries)

"""
    /** 
     * hdfsGetPathInfo - Get information about a path as a (dynamically
     * allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
     * called when the pointer is no longer needed.
     * @param fs The configured filesystem handle.
     * @param path The path of the file. 
     * @return Returns a dynamically-allocated hdfsFileInfo object;
     * NULL on error.
     */
    hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path);


    /** 
     * hdfsFreeFileInfo - Free up the hdfsFileInfo array (including fields) 
     * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
     * objects.
     * @param numEntries The size of the array.
     */
    void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries);



    /** 
     * hdfsGetHosts - Get hostnames where a particular block (determined by
     * pos & blocksize) of a file is stored. The last element in the array
     * is NULL. Due to replication, a single block could be present on
     * multiple hosts.
     * @param fs The configured filesystem handle.
     * @param path The path of the file. 
     * @param start The start of the block.
     * @param length The length of the block.
     * @return Returns a dynamically-allocated 2-d array of blocks-hosts;
     * NULL on error.
     */
    char*** hdfsGetHosts(hdfsFS fs, const char* path, 
            tOffset start, tOffset length);


    /** 
     * hdfsFreeHosts - Free up the structure returned by hdfsGetHosts
     * @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
     * objects.
     * @param numEntries The size of the array.
     */
    void hdfsFreeHosts(char ***blockHosts);
"""


# tOffset hdfsGetDefaultBlockSize(hdfsFS fs);
libhdfs.hdfsGetDefaultBlockSize.argtypes = [hdfsFS]
libhdfs.hdfsGetDefaultBlockSize.restype = tOffset
def hdfsGetDefaultBlockSize(fs):
  """Get the optimum blocksize.

  @param fs The configured filesystem handle.
  @return Returns the blocksize; -1 on error.
  """
  raise NotImplementedError, "TODO(travis)"

# tOffset hdfsGetCapacity(hdfsFS fs);
libhdfs.hdfsGetCapacity.argtypes = [hdfsFS]
libhdfs.hdfsGetCapacity.restype = tOffset
def hdfsGetCapacity(fs):
  """Return the raw capacity of the filesystem.

  @param fs The configured filesystem handle.
  @return Returns the raw-capacity; -1 on error.
  """
  raise NotImplementedError, "TODO(travis)"

# tOffset hdfsGetUsed(hdfsFS fs);
libhdfs.hdfsGetUsed.argtypes = [hdfsFS]
libhdfs.hdfsGetUsed.restype = tOffset
def hdfsGetUsed(fs):
  """Return the total raw size of all files in the filesystem.

  @param fs The configured filesystem handle.
  @return Returns the total-size; -1 on error.
  """
  raise NotImplementedError, "TODO(travis)"

# int hdfsChown(hdfsFS fs, const char* path, const char *owner, const char *group);
libhdfs.hdfsChown.argtypes = [hdfsFS, c_char_p, c_char_p, c_char_p]
libhdfs.hdfsChown.restype = c_int
def hdfsChown(fs, path, owner, group):
  """
  @param fs The configured filesystem handle.
  @param path the path to the file or directory
  @param owner this is a string in Hadoop land. Set to null or "" if only setting group
  @param group  this is a string in Hadoop land. Set to null or "" if only setting user
  @return 0 on success else -1
  """
  raise NotImplementedError, "TODO(travis)"

# int hdfsChmod(hdfsFS fs, const char* path, short mode);
libhdfs.hdfsChmod.argtypes = [hdfsFS, c_char_p, c_short]
libhdfs.hdfsChmod.restype = c_int
def hdfsChmod(fs, path, mode):
  """
  @param fs The configured filesystem handle.
  @param path the path to the file or directory
  @param mode the bitmask to set it to
  @return 0 on success else -1
  """
  raise NotImplementedError, "TODO(travis)"

# int hdfsUtime(hdfsFS fs, const char* path, tTime mtime, tTime atime);
libhdfs.hdfsUtime.argtypes = [hdfsFS, c_char_p, tTime, tTime]
libhdfs.hdfsUtime.restype = c_int
def hdfsUtime(fs, path, mtime, atime):
  """
  @param fs The configured filesystem handle.
  @param path the path to the file or directory
  @param mtime new modification time or 0 for only set access time in seconds
  @param atime new access time or 0 for only set modification time in seconds
  @return 0 on success else -1
  """
  raise NotImplementedError, "TODO(travis)"

# EOF