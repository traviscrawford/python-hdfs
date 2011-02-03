from hdfs._common import *

class Hfilesystem(object):

  def __init__(self, hostname='default', port=0):
    self.hostname = hostname
    self.port = port
    self.fs = libhdfs.hdfsConnect(hostname, port)

  def __del__(self):
    if self.fs:
      self.disconnect()

  def capacity(self):
    """Return the raw capacity of the filesystem.

    @param fs The configured filesystem handle.
    @return Returns the raw-capacity; None on error.
    """
    cap = libhdfs.hdfsGetCapacity(self.fs)
    if cap != -1:
      return cap
    else:
      return None

  def chmod(self, path, mode):
    """Change file mode.

    Permissions in HDFS are POSIX-like, with some important differences.

    For more information, please see:
    http://hadoop.apache.org/hdfs/docs/current/hdfs_permissions_guide.html

    @param path the path to the file or directory
    @param mode the bitmask to set it to
    @return True on success else False
    """
    if libhdfs.hdfsChmod(self.fs, path, mode) == 0:
      return True
    else:
      return False

  def chown(self, path, owner, group):
    """Change owner and group for a file.

    @param path the path to the file or directory
    @param owner this is a string in Hadoop land. Set to None if only setting group
    @param group  this is a string in Hadoop land. Set to None if only setting user
    @return Returns True on success, False on error.
    """
    if libhdfs.hdfsChown(self.fs, path, owner, group) == 0:
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
    self.fs = libhdfs.hdfsConnect(hostname, port)
    if not self.fs:
      raise HdfsError('Failed connecting to %s:%d' % (hostname, port))

  def copy(self, srcPath, dstPath):
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
    if libhdfs.hdfsDelete(self.fs, path) == 0:
      return True
    else:
      return False

  def disconnect(self):
    if libhdfs.hdfsDisconnect(self.fs) == -1:
      raise HdfsError('Failed disconnecting from %s:%d' % (self.hostname, self.port))

  def exists(self, path):
    """
    @param fs The configured filesystem handle.
    @param path The path to look for
    @return Returns True on success, False on error.
    """
    if libhdfs.hdfsExists(self.fs, path) == 0:
      return True
    else:
      return False

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

  # TODO(travis): Decorate with @exists
  def listdir(self, path):
    """Get list of files/directories for a given directory-path.
    hdfsFreeFileInfo should be called to deallocate memory.

    @param path The path of the directory.
    @return Returns a dynamically-allocated array of hdfsFileInfo objects;
    NULL on error.
    """
    if not self.exists(path):
      return None

    path = c_char_p(path)
    num_entries = c_int()
    entries = []
    entries_p = libhdfs.hdfsListDirectory(self.fs, path, pointer(num_entries))
    [entries.append(entries_p[i].mName) for i in range(num_entries.value)]
    return sorted(entries)

  def mkdir(self, path):
    """Make the given file and all non-existent parents into directories.

    @param fs The configured filesystem handle.
    @param path The path of the directory.
    @return Returns True on success, False on error.
    """
    if libhdfs.hdfsCreateDirectory(self.fs, path) == 0:
      return True
    else:
      return False

  def move(self, srcFS, srcPath, dstFS, dstPath):
    """Move file from one filesystem to another.

    @param srcFS The handle to source filesystem.
    @param src The path of source file.
    @param dstFS The handle to destination filesystem.
    @param dst The path of destination file.
    @return Returns 0 on success, -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def rename(self, oldPath, newPath):
    """
    @param fs The configured filesystem handle.
    @param oldPath The path of the source file.
    @param newPath The path of the destination file.
    @return Returns 0 on success, -1 on error.
    """
    if libhdfs.hdfsRename(self.fs, oldPath, newPath) == 0:
      return True
    else:
      return False

  def set_replication(self, path, replication):
    """Set the replication of the specified file to the supplied value.

    @param fs The configured filesystem handle.
    @param path The path of the file.
    @return Returns 0 on success, -1 on error.
    """
    raise NotImplementedError, "TODO(travis)"

  def stat(self, path):
    """Get file status.

    @param path The path of the file.
    @return Returns a hdfsFileInfo structure.
    """
    return libhdfs.hdfsGetPathInfo(self.fs, path).contents

  def getHosts(self, path, begin, offset):
    '''Get host list.
    '''
    r= libhdfs.hdfsGetHosts(self.fs, path, begin, offset)
    i=0
    ret = []
    while r[0][i]:
       ret.append(r[0][i])
       i+=1
    if r:
      libhdfs.hdfsFreeHosts(r)
    return ret
