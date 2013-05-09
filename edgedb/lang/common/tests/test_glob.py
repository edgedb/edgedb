##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import glob


class TestUtilsGlob:
    def test_utils_glob_module(self):
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
