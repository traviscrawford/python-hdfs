#!/usr/bin/python26

import unittest
from datetime import datetime
from hdfs import *

NN_HOST = 'hadoop.twitter.com'
NN_PORT = 8020


class BasicTestCase(unittest.TestCase):
  def test_basic_functionality(self):
    path = '/user/travis/test_%s' % datetime.now().strftime('%Y%m%dT%H%M%SZ')
    data = 'read write test'

    fs = HDFS('hadoop.twitter.com', 8020)

    fh = fs.open(path, 'w')
    bytes_written = fs.write(fh, data)
    self.assertEqual(bytes_written, len(data))
    fs.close(fh)

    fh = fs.open(path)
    self.assertTrue(fs.seek(fh, 10))
    self.assertEqual(fs.tell(fh), 10)
    fs.seek(fh, 0)
    read_data = fs.read(fh)
    self.assertEqual(read_data, data)
    fs.close(fh)

    self.assertTrue(fs.exists(path))
    self.assertFalse(fs.exists(path + 'doesnotexist'))

    self.assertTrue(fs.rename(path, path + 'renamed'))

    self.assertTrue(fs.delete(path + 'renamed'))
    self.assertFalse(fs.delete(path + 'doesnotexist'))

"""
class ListDirectoryTestCase(unittest.TestCase):
  def setUp(self):
    self.fs = hdfsConnect('hadoop.twitter.com', 8020)

  def test_list_diretory(self):
    entries = hdfsListDirectory(self.fs, '/user')
    self.assertTrue(entries)

  def test_list_missing_directory(self):
    entries = hdfsListDirectory(self.fs, '/doesnotexist')
    self.assertEqual(entries, None)

  def tearDown(self):
    hdfsDisconnect(self.fs)

class RenameTestCase(unittest.TestCase):
  def test_rename(self):
    path = '/user/travis/test_%s' % datetime.now().strftime('%Y%m%dT%H%M%SZ')
"""

if __name__ == '__main__':
  test_cases = [BasicTestCase,
               ]
  for test_case in test_cases:
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    unittest.TextTestRunner(verbosity=2).run(suite)

# EOF
