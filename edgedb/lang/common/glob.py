##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re


class ModuleGlobPattern:
    """Matches module names against the specified pattern.

       The following are the matching rules:

          **.    - empty string or any string ending with a dot
          .**    - empty string or any string beginning with a dot
          **     - any string (including zero-length)
          *.     - empty string or any string before the first dot (dot included)
          .*     - empty string or any string after the dot and before the
                   next dot (first dot included)
          *      - any string not including a dot (including zero-length)

          ++.    - any string ending with a dot
          .++    - any string beginning with a dot
          ++     - any non zero-length string
          +.     - any string before the first dot (dot included)
          .+     - any string after the dot and before the next dot
                   (first dot included)
          +      - any non zero-length string not including a dot

    """

    def __init__(self, pattern):
        self._pattern_source = pattern

        pattern = re.escape(pattern)

        pattern = (pattern.replace(r'\*\*\.', r'(|.+\.)')
                          .replace(r'\.\*\*', r'(|\..+)')
                          .replace(r'\*\*', r'.*')
                          .replace(r'\*\.', r'(|[^\.]+\.)')
                          .replace(r'\.\*', r'(|\.[^\.]+)')
                          .replace(r'\*', r'[^\.]*')

                          .replace(r'\+\+\.', r'.+\.')
                          .replace(r'\.\+\+', r'\..+')
                          .replace(r'\+\+', r'.+')
                          .replace(r'\+\.', r'[^\.]+\.')
                          .replace(r'\.\+', r'\.[^\.]+')
                          .replace(r'\+', r'[^\.]+'))

        self._pattern = re.compile('^' + pattern + '$')

    def match(self, string):
        """Returns True if the specified string can be matched by this pattern"""
        return bool(self._pattern.match(string))

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self._pattern_source)
