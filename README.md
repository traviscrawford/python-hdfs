Python HDFS Client
==================

Use the Hadoop distributed filesystem directly from Python! Implemented as
a file-like object, working with HDFS files feels similar to how you'd expect.

Quick Start
-----------

    from hdfs.hfile import Hfile

    hfile = Hfile(hostname, port, path, mode='w')
    hfile.write(data)
    hfile.close()

    hfile = Hfile(hostname, port, path)
    data = hfile.read()
    hfile.close()

See example.py for more help getting started.

Development Status
------------------

The library is under active development and should not be used in production
at this time. The ctypes layer wrapping the native client works correctly, as
do critical methods like read and write. Areas under development include
implementing more Hfilesystem methods, and general polish.

Contributing
------------

Contributions are extremely welcome, as are suggestions about how to structure,
package, and test the library. Feel free to send me an email or pull request
and I'll get back ASAP.