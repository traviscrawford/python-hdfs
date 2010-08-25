Python HDFS Client
==================

Use the Hadoop distributed filesystem directly from Python! Implemented as
a file-like object, working with HDFS files feels similar to how you'd expect.

Quick Start
-----------

    import hdfs

    hfile = hdfs.File(hostname, port, path, mode='w')
    hfile.write('foo')
    hfile.close()

    hfile = hdfs.File(hostname, port, path)
    data = hfile.read()
    hfile.close()

Development Status
------------------

The library is under active development and should not be used in production
at this time. The ctypes layer wrapping the native client works correctly, as
do critical methods like read and write. Areas under development include
iteration, a Filesystem class, and general polish.

Contributing
------------

Contributions are extremely welcome, as are suggestions about how to structure,
package, and test the library. Feel free to send me an email or pull request
and I'll get back ASAP.