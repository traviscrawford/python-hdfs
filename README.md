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

Java Classpath
--------------

As libhdfs uses JNI, we must set the classpath environment variable for
python-hdfs to function properly. The exact classpath will vary between
installations, but basically you need the Hadoop conf directory and all
Hadoop jar's. For example:

    HADOOP_CONF=/etc/hadoop-0.20/conf
    HADOOP_HOME=/usr/lib/hadoop-0.20
    export CLASSPATH=${HADOOP_CONF}:$(find ${HADOOP_HOME} -name *.jar | tr '\n' ':')

After exporting the classpath simply start your process using python-hdfs
as usual.

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