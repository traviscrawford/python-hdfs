#!/usr/bin/python26

import unittest
from datetime import datetime
from hdfs import *

hostname = 'hadoop.twitter.com'
port = 8020

class BasicTestCase(unittest.TestCase):
  def test_basic_functionality(self):
    path = '/user/travis/test_%s' % datetime.now().strftime('%Y%m%dT%H%M%SZ')
    data = 'read write test'

    fs = HDFS(hostname, port)

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

if __name__ == '__main__':
  test_cases = [BasicTestCase,
               ]
  for test_case in test_cases:
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    unittest.TextTestRunner(verbosity=2).run(suite)
