from hdfs._common import *

class Hfile(object):

  def __init__(self, hostname, port, filename, mode='r', buffer_size=0,
               replication=0, block_size=0):
    flags = None
    if mode == 'r':
      flags = os.O_RDONLY
    elif mode == 'w':
      flags = os.O_WRONLY
    else:
      raise HdfsError('Invalid open flags.')
    self.hostname = hostname
    self.port = port
    self.filename = filename
    self.fs = libhdfs.hdfsConnect(hostname, port)
    self.fh = libhdfs.hdfsOpenFile(self.fs, filename, flags, buffer_size,
                                   replication, block_size)
    self.readline_pos = 0

  def __iter__(self):
    return self

  def close(self):
    libhdfs.hdfsCloseFile(self.fs, self.fh)
    libhdfs.hdfsDisconnect(self.fs)

  def next(self):
    line = self.readline()
    if not line:
      raise StopIteration
    return line

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
    flags = None
    if mode == 'r':
      flags = os.O_RDONLY
    elif mode == 'w':
      flags = os.O_WRONLY
    else:
      raise HdfsError('Invalid open flags.')

    self.fh = libhdfs.hdfsOpenFile(self.fs, filename, flags, buffer_size,
                                   replication, block_size)
    if not self.fh:
      raise HdfsError('Failed opening %s' % filename)

  def pread(self, position, length):
    """Positional read of data from an open file.

    @param position Position from which to read
    @param length The length of the buffer.
    @return Returns the number of bytes actually read, possibly less than
    than length; None on error.
    """
    st = self.stat()
    if position >= st.mSize:
      return None

    buf = create_string_buffer(length)
    buf_p = cast(buf, c_void_p)

    ret = libhdfs.hdfsPread(self.fs, self.fh, position, buf_p, length)
    if ret == -1:
      raise HdfsError('read failure')
    return buf.value

  def read(self):
    st = self.stat()

    buf = create_string_buffer(st.mSize)
    buf_p = cast(buf, c_void_p)

    ret = libhdfs.hdfsRead(self.fs, self.fh, buf_p, st.mSize)
    if ret == -1:
      raise HdfsError('read failure')
    return buf.value[0:ret]

  def readline(self, length=100):
    line = ''
    while True:
      data = self.pread(self.readline_pos, length)
      if data is None:
        return line
      newline_pos = data.find('\n')
      if newline_pos == -1:
        self.readline_pos += len(data)
        line += data
      else:
        self.readline_pos += newline_pos+1
        return line + data[0:newline_pos+1]

  def readlines(self):
    return [line for line in self]

  def seek(self, position):
    """Seek to given offset in file. This works only for
    files opened in read-only mode.

    Returns True if seek was successful, False on error.
    """
    if libhdfs.hdfsSeek(self.fs, self.fh, position) == 0:
      return True
    else:
      return False

  def stat(self):
    return libhdfs.hdfsGetPathInfo(self.fs, self.filename).contents

  def tell(self):
    """Returns current offset in bytes, None on error."""
    ret = libhdfs.hdfsTell(self.fs, self.fh)
    if ret != -1:
      return ret
    else:
      return None

  def write(self, buffer):
    sb = create_string_buffer(buffer)
    buffer_p = cast(sb, c_void_p)

    ret = libhdfs.hdfsWrite(self.fs, self.fh, buffer_p, len(buffer))

    if ret != -1:
      return True
    else:
      return False
