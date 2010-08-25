#!/usr/bin/python26

import hdfs
import unittest
from datetime import datetime
from StringIO import StringIO

hostname = 'hadoop.twitter.com'
port = 8020
path = '/user/travis/test_%s' % datetime.now().strftime('%Y%m%dT%H%M%SZ')
data = 'read write test'


class FilesystemTestCase(unittest.TestCase):

  def test_filesystem(self):
    fs = hdfs.Filesystem(hostname, port)
    self.assertTrue(fs.exists(path))
    self.assertFalse(fs.exists(path + 'doesnotexist'))

    self.assertTrue(fs.rename(path, path + 'renamed'))

    self.assertTrue(fs.delete(path + 'renamed'))
    self.assertFalse(fs.delete(path))


class FileTestCase(unittest.TestCase):

  def test_file(self):
    hfile = hdfs.File(hostname, port, path, mode='w')
    self.assertTrue(hfile.write(data))
    hfile.close()

    hfile = hdfs.File(hostname, port, path)

    self.assertTrue(hfile.seek(10))
    self.assertEqual(hfile.tell(), 10)
    hfile.seek(0)

    read_data = hfile.read()
    self.assertEqual(read_data, data)

    hfile.close()

  def test_iter(self):
    data = StringIO('a\nb\nc')
    hfile = hdfs.File(hostname, port, path, mode='w')
    for line in data:
      self.assertTrue(hfile.write(line))
    hfile.close()

    hfile = hdfs.File(hostname, port, path)
    for line in hfile:
      print "==> Read line: %s" % line

    #self.assertEqual(data, read_data)

if __name__ == '__main__':
  test_cases = [#FilesystemTestCase,
                FileTestCase,
               ]
  for test_case in test_cases:
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    unittest.TextTestRunner(verbosity=2).run(suite)
