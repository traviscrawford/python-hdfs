#!/usr/bin/env python26

import unittest
from datetime import datetime
from hdfs.hfilesystem import Hfilesystem
from hdfs.hfile import Hfile

hostname = 'hadoop.twitter.com'
port = 8020
path = '/user/travis/test_%s' % datetime.now().strftime('%Y%m%dT%H%M%SZ')
data = 'read write test'


class HfilesystemTestCase(unittest.TestCase):

  def test_filesystem(self):
    hfile = Hfile(hostname, port, path, mode='w')
    hfile.close()

    fs = Hfilesystem(hostname, port)

    self.assertTrue(fs.exists(path))
    self.assertFalse(fs.exists(path + 'doesnotexist'))

    self.assertTrue(fs.rename(path, path + 'renamed'))

    self.assertTrue(fs.delete(path + 'renamed'))
    self.assertFalse(fs.delete(path))

  def test_mkdir(self):
    fs = Hfilesystem(hostname, port)
    self.assertTrue(fs.mkdir(path))
    self.assertTrue(fs.delete(path))


if __name__ == '__main__':
  test_cases = [HfilesystemTestCase,
               ]
  for test_case in test_cases:
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    unittest.TextTestRunner(verbosity=2).run(suite)
