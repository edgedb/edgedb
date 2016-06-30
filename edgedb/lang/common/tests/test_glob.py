##
# Copyright (c) 2013 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.common import glob


class TestUtilsGlob:
    def test_utils_glob_module_one(self):
        tests = {
            'com.umbrella.corp.projects.tvirus.evil': {
                '**': True,
                '**.tvirus.**': True,
                '*.tvirus.**': False,
                'com.umbrella.**': True,
                'com.*': False,
                'com.umb**rella.**': True,
                '*.umbrella.**': True,
                '++': True,
                '++.com.umbrella.corp.projects.tvirus.evil': False,
                'com.umbrella.corp.projects.tvirus.evil.++': False,
                '++.com.**': False,
                '**.com.++': True,
                'com.+.projects.**': False,
                'com.+.corp.**': True
            },

            '': {
                '**': True,
                '*': True,
                '++': False,
                '+': False
            }
        }

        for name, patterns in tests.items():
            for pattern, expected in patterns.items():
                assert glob.ModuleGlobPattern(pattern).match(name) == expected,  \
                       "failed: {!r} on {!r}".format(pattern, name)

    def test_utils_glob_module_set(self):
        tests = {
            'com.umbrella.corp.projects.tvirus.evil': {
                ('**', '**.tvirus.**', '*.tvirus.**'): True,
                ('*.tvirus.**', 'com.*'): False,
            },

            '': {
                ('**', '+'): True,
                ('+', '++'): False
            }
        }

        for name, patterns in tests.items():
            for pattern, expected in patterns.items():
                gl = glob.ModuleGlobPatternSet(pattern)

                assert gl.match(name) == expected,  \
                       "failed: {!r} on {!r}".format(pattern, name)
