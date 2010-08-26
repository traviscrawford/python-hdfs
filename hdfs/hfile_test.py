#!/usr/bin/env python26

import unittest
from datetime import datetime
from hdfs.hfile import Hfile

hostname = 'hadoop.twitter.com'
port = 8020
path = '/user/travis/test_%s' % datetime.now().strftime('%Y%m%dT%H%M%SZ')
data = 'read write test'


class FileTestCase(unittest.TestCase):

  def test_file(self):
    hfile = Hfile(hostname, port, path, mode='w')
    self.assertTrue(hfile.write(data))
    hfile.close()

    hfile = Hfile(hostname, port, path)

    self.assertTrue(hfile.seek(10))
    self.assertEqual(hfile.tell(), 10)
    hfile.seek(0)

    read_data = hfile.read()
    self.assertEqual(read_data, data)

    hfile.close()

  def test_iter_with_trailing_newline(self):
    write_data = 'a\nb\nc\n'
    hfile = Hfile(hostname, port, path, mode='w')
    self.assertTrue(hfile.write(write_data))
    hfile.close()

    hfile = Hfile(hostname, port, path)
    read_data = ''
    for line in hfile:
      read_data += line

    self.assertEqual(write_data, read_data)

  def test_iter_without_trailing_newline(self):
    write_data = 'a\nb\nc'
    hfile = Hfile(hostname, port, path, mode='w')
    self.assertTrue(hfile.write(write_data))
    hfile.close()

    hfile = Hfile(hostname, port, path)
    read_data = ''
    for line in hfile:
      read_data += line

    self.assertEqual(write_data, read_data)

if __name__ == '__main__':
  test_cases = [FileTestCase,
               ]
  for test_case in test_cases:
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    unittest.TextTestRunner(verbosity=2).run(suite)
