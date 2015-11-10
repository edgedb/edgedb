##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re


def _translate_pattern(pattern):
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

    return '^' + pattern + '$'


class _MatchMixin:
    def match(self, string):
        """Returns True if the specified string can be matched by this pattern"""
        return bool(self._pattern.match(string))

    def match_all(self, strings):
        """Returns True if all specified strings can be matched by this pattern"""
        return all(self._pattern.match(s) for s in strings)

    def match_any(self, strings):
        """Returns True if any of the specified strings can be matched by this pattern"""
        return any(self._pattern.match(s) for s in strings)


class ModuleGlobPattern(_MatchMixin):
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
        self._pattern = re.compile(_translate_pattern(pattern))

    def __eq__(self, other):
        if not isinstance(other, ModuleGlobPattern):
            return False
        else:
            return self._pattern_source == other._pattern_source

    def __hash__(self):
        return hash(self._pattern_source)

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self._pattern_source)

    def __mm_serialize__(self):
        return self._pattern_source


class ModuleGlobPatternSet(frozenset, _MatchMixin):
    """Matches module names against the specified set of patterns."""

    def __new__(cls, patterns):
        sources = []
        exprs = []

        for p in patterns:
            if not isinstance(p, ModuleGlobPattern):
                p = ModuleGlobPattern(p)

            sources.append(p._pattern_source)
            exprs.append(p)

        s = super().__new__(cls, exprs)

        res = []

        s._pattern_source = frozenset(sources)
        s._is_universal = False

        for pattern in s._pattern_source:
            if pattern == '**':
                s._is_universal = True

            res.append(_translate_pattern(pattern))

        s._pattern = re.compile('|'.join(res))

        return s

    def match(self, string):
        return bool(self._pattern.match(string))

    @property
    def is_universal(self):
        """True if this pattern set matches any string"""
        return self._is_universal

    def __mm_serialize__(self):
        return list(self._pattern_source)
