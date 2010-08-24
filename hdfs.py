#!/usr/bin/python26

"""
This wrapper works with:

c0e06197395055711483fa1c747bd8cd  /usr/include/hdfs.h
"""

import logging
import os

from ctypes import *

class HdfsError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

tSize = c_int32
tTime = c_long # double-check this
tOffset = c_int64
tPort = c_uint16
hdfsFS = c_void_p
hdfsFile = c_void_p

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

class HDFS(object):

  def __init__(self, hostname='default', port=0):
    if not os.getenv("CLASSPATH"):
      raise HdfsError('Failed loading libhdfs.so because CLASSPATH environment variable is not set.')

    self._libhdfs = cdll.LoadLibrary('libhdfs.so')

    self._libhdfs.hdfsAvailable.argtypes = [hdfsFS, hdfsFile]
    self._libhdfs.hdfsChmod.argtypes = [hdfsFS, c_char_p, c_short]
    self._libhdfs.hdfsChown.argtypes = [hdfsFS, c_char_p, c_char_p, c_char_p]
    self._libhdfs.hdfsCloseFile.argtypes = [hdfsFS, hdfsFile]
    self._libhdfs.hdfsConnect.argtypes = [c_char_p, tPort]
    self._libhdfs.hdfsConnect.restype = hdfsFS
    self._libhdfs.hdfsCopy.argtypes = [hdfsFS, c_char_p, hdfsFS, c_char_p]
    self._libhdfs.hdfsDelete.argtypes = [hdfsFS, c_char_p]
    self._libhdfs.hdfsDisconnect.argtypes = [hdfsFS]
    self._libhdfs.hdfsExists.argtypes = [hdfsFS, c_char_p]
    self._libhdfs.hdfsFlush.argtypes = [hdfsFS, hdfsFile]
    self._libhdfs.hdfsGetCapacity.argtypes = [hdfsFS]
    self._libhdfs.hdfsGetCapacity.restype = tOffset
    self._libhdfs.hdfsGetDefaultBlockSize.argtypes = [hdfsFS]
    self._libhdfs.hdfsGetDefaultBlockSize.restype = tOffset
    self._libhdfs.hdfsGetUsed.argtypes = [hdfsFS]
    self._libhdfs.hdfsGetUsed.restype = tOffset
    self._libhdfs.hdfsListDirectory.argtypes = [hdfsFS, c_char_p, POINTER(c_int)]
    self._libhdfs.hdfsListDirectory.restype = POINTER(hdfsFileInfo)
    self._libhdfs.hdfsMove.argtypes = [hdfsFS, c_char_p, hdfsFS, c_char_p]
    self._libhdfs.hdfsOpenFile.argtypes = [hdfsFS, c_char_p, c_int, c_int, c_short, tSize]
    self._libhdfs.hdfsOpenFile.restype = hdfsFile
    self._libhdfs.hdfsRead.argtypes = [hdfsFS, hdfsFile, c_void_p, tSize]
    self._libhdfs.hdfsRead.restype = tSize
    self._libhdfs.hdfsRename.argtypes = [hdfsFS, c_char_p, c_char_p]
    self._libhdfs.hdfsSeek.argtypes = [hdfsFS, hdfsFile, tOffset]
    self._libhdfs.hdfsTell.argtypes = [hdfsFS, hdfsFile]
    self._libhdfs.hdfsTell.restype = tOffset
    self._libhdfs.hdfsUtime.argtypes = [hdfsFS, c_char_p, tTime, tTime]
    self._libhdfs.hdfsWrite.argtypes = [hdfsFS, hdfsFile, c_void_p, tSize]
    self._libhdfs.hdfsWrite.restype = tSize

    self.fs = self.connect(hostname, port)

  def __del__(self):
    self.disconnect()

  def available(self, fh):
    """Number of bytes that can be read from this input stream without blocking.

    @param fs The configured filesystem handle.
    @param file The file handle.
    @return Returns available bytes; -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def chmod(self, path, mode):
    """
    @param fs The configured filesystem handle.
    @param path the path to the file or directory
    @param mode the bitmask to set it to
    @return 0 on success else -1
    """
    raise NotImplementedError, "TODO(travis)"

  def chown(self, path, owner, group):
    """
    @param fs The configured filesystem handle.
    @param path the path to the file or directory
    @param owner this is a string in Hadoop land. Set to null or "" if only setting group
    @param group  this is a string in Hadoop land. Set to null or "" if only setting user
    @return 0 on success else -1
    """
    raise NotImplementedError, "TODO(travis)"

  def close(self, fh):
    """
    @param fs The configured filesystem handle.
    @param fh The file handle.
    @return Returns True on success, False on error.
    """
    if (self._libhdfs.hdfsCloseFile(self.fs, fh) == 0):
      return True
    else:
      return False

  def connect(self, hostname, port):
    """Connect to a hdfs file system.

    @param host A string containing either a host name, or an ip address
    of the namenode of a hdfs cluster. 'host' should be passed as NULL if
    you want to connect to local filesystem. 'host' should be passed as
    'default' (and port as 0) to used the 'configured' filesystem
    (core-site/core-default.xml).
    @param port The port on which the server is listening.
    @return Returns a handle to the filesystem or NULL on error.
    """
    fs = self._libhdfs.hdfsConnect(hostname, port)
    if not fs:
      raise HdfsError('Failed connecting to %s:%d' % (hostname, port))
    return fs

  def copy(self, srcFS, srcPath, dstFS, dstPath):
    """Copy file from one filesystem to another.
    @param srcFS The handle to source filesystem.
    @param src The path of source file.
    @param dstFS The handle to destination filesystem.
    @param dst The path of destination file.
    @return Returns 0 on success, -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def delete(self, path):
    """
    @param fs The configured filesystem handle.
    @param path The path of the file.
    @return Returns 0 on success, -1 on error.
    """
    if self._libhdfs.hdfsDelete(self.fs, path) == 0:
      return True
    else:
      return False

  def disconnect(self):
    """
    @param fs The configured filesystem handle.
    @return Returns True on success, False on error.
    """
    if self._libhdfs.hdfsDisconnect(self.fs) == 0:
      return True
    else:
      return False

  def exists(self, path):
    """
    @param fs The configured filesystem handle.
    @param path The path to look for
    @return Returns True on success, False on error.
    """
    if self._libhdfs.hdfsExists(self.fs, path) == 0:
      return True
    else:
      return False

  def flush(self, fh):
    """Flush the data.

    * @param fs The configured filesystem handle.
    * @param file The file handle.
    * @return Returns 0 on success, -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def get_capacity(self, fs):
    """Return the raw capacity of the filesystem.

    @param fs The configured filesystem handle.
    @return Returns the raw-capacity; -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def get_default_block_size(self):
    """Get the optimum blocksize.

    @param fs The configured filesystem handle.
    @return Returns the blocksize; -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def get_used(self):
    """Return the total raw size of all files in the filesystem.

    @param fs The configured filesystem handle.
    @return Returns the total-size; -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def listdir(self, path):
    """Get list of files/directories for a given directory-path.
    hdfsFreeFileInfo should be called to deallocate memory.

    @param fs The configured filesystem handle.
    @param path The path of the directory.
    @param numEntries Set to the number of files/directories in path.
    @return Returns a dynamically-allocated array of hdfsFileInfo objects;
    NULL on error.
    """
    if not hdfsExists(self, path):
      return None

    path = c_char_p(path)
    num_entries = c_int()
    entries = []
    entries_p = self._libhdfs.hdfsListDirectory(self.fs, path, pointer(num_entries))
    [entries.append(entries_p[i].mName) for i in range(num_entries.value)]
    return sorted(entries)

  def move(self, srcFS, srcPath, dstFS, dstPath):
    """Move file from one filesystem to another.

    @param srcFS The handle to source filesystem.
    @param src The path of source file.
    @param dstFS The handle to destination filesystem.
    @param dst The path of destination file.
    @return Returns 0 on success, -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def open(self, filename, mode='r', buffer_size=0,
           replication=0, block_size=0):
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
    if not self.fs:
      raise HdfsError('No filesystem!')

    flags = None
    if mode == 'r':
      flags = os.O_RDONLY
    elif mode == 'w':
      flags = os.O_WRONLY
    else:
      raise HdfsError('Invalid open flags.')

    fh = self._libhdfs.hdfsOpenFile(self.fs, filename, flags, buffer_size,
                                    replication, block_size)
    if not fh:
      raise HdfsError('Failed opening <%s>' % filename)

    return fh

  def read(self, fh):
    """
    @param fs The configured filesystem handle.
    @param file The file handle.
    @param buffer The buffer to copy read bytes into.
    @param length The length of the buffer.
    @return Returns the number of bytes actually read, possibly less
    than than length;-1 on error.
    """
    if not self.fs:
      raise HdfsError('No filesystem!')
    if not fh:
      raise HdfsError('No file handle!')

    buffer = create_string_buffer(1024*1024)
    buffer_p = cast(buffer, c_void_p)

    ret = self._libhdfs.hdfsRead(self.fs, fh, buffer_p, 1024*1024)
    if ret == -1:
      raise HdfsError('read failure')
    return buffer.value[0:ret]

  def rename(self, oldPath, newPath):
    """
    @param fs The configured filesystem handle.
    @param oldPath The path of the source file.
    @param newPath The path of the destination file.
    @return Returns 0 on success, -1 on error.
    """
    if self._libhdfs.hdfsRename(self.fs, oldPath, newPath) == 0:
      return True
    else:
      return False

  def seek(self, fh, desired_pos):
    """Seek to given offset in file. This works only for
    files opened in read-only mode.

    @param fs The configured filesystem handle.
    @param file The file handle.
    @param desiredPos Offset into the file to seek into.
    @return Returns 0 on success, -1 on error.
    """
    if self._libhdfs.hdfsSeek(self.fs, fh, desired_pos) == 0:
      return True
    else:
      return False

  def tell(self, fh):
    """
    @param fs The configured filesystem handle.
    @param fh The file handle.
    @return Current offset in bytes, None on error.
    """
    ret = self._libhdfs.hdfsTell(self.fs, fh)
    if ret != -1:
      return ret
    else:
      return None

  def utime(self, path, mtime, atime):
    """
    @param fs The configured filesystem handle.
    @param path the path to the file or directory
    @param mtime new modification time or 0 for only set access time in seconds
    @param atime new access time or 0 for only set modification time in seconds
    @return 0 on success else -1
    """
    raise NotImplementedError, "TODO(travis)"

  def write(self, fh, buffer):
    """Write data into an open file.

    @param fs The configured filesystem handle.
    @param file The file handle.
    @param buffer The data.
    @param length The no. of bytes to write.
    @return Returns the number of bytes written, -1 on error.
    """
    sb = create_string_buffer(buffer)
    buffer_p = cast(sb, c_void_p)

    ret = self._libhdfs.hdfsWrite(self.fs, fh, buffer_p, len(buffer))

    if ret == -1:
      raise HdfsError('write failure')
    return ret
