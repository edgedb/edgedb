##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import re


class NamespaceGlobPattern:
    def __init__(self, pattern, *, separator):
        if not isinstance(separator, str) or len(separator) != 1:
            raise ValueError('glob: separator must be a single-character string')

        if separator in {'*', '?'}:
            raise ValueError("glob: separator cannot be '*' or '?'")

        self._pattern_source = pattern

        pattern = re.escape(pattern)

        pattern = pattern.replace(r'\*\*', '.+') \
                         .replace(r'\*', '[^{}]+'.format(re.escape(separator))) \
                         .replace(r'\?', '.')

        self._separator = separator
        self._pattern = re.compile('^' + pattern + '$')

    @property
    def separator(self):
        return self._separator

    def match(self, string):
        return bool(self._pattern.match(string))

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self._pattern_source)


class ModuleGlobPattern(NamespaceGlobPattern):
    def __init__(self, pattern):
        super().__init__(pattern, separator='.')
