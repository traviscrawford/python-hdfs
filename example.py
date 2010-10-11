#!/usr/bin/env python26

"""Python HDFS use examples.

After reading this example you should have enough information to read and write
HDFS files from your programs.
"""

from hdfs.hfile import Hfile

hostname = 'hadoop.twitter.com'
port = 8020
hdfs_path = '/user/travis/example'
local_path = '/etc/motd'

# Let's open local and HDFS files.

hfile = Hfile(hostname, port, hdfs_path, mode='w')
fh = open(local_path)

# Now we'll write lines from a local file into the HDFS file.
for line in fh:
  hfile.write(line)

# And close them.
fh.close()
hfile.close()

# Let's read local_path into memory for comparison.
motd = open(local_path).read()

# Now let's read the data back
hfile = Hfile(hostname, port, hdfs_path)

# With an iterator
data_read_from_hdfs = ''
for line in hfile:
  data_read_from_hdfs += line
print motd == data_read_from_hdfs

# All at once
data_read_from_hdfs = hfile.read()
print motd == data_read_from_hdfs

hfile.close()

# Hopefully you have enough info to get started!

from hdfs.hfilesystem import Hfilesystem
hfs = Hfilesystem(hostname, port)
print hfs.getHosts(hdfs_path, 0, 1)

