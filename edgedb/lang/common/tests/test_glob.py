##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import glob


class TestUtilsGlob:
    def test_utils_glob_module(self):
        name = 'com.umbrella.corp.projects.tvirus.evil'

        tests = {
            '**': True,
            '**.tvirus.**': True,
            '*.tvirus.**': False,
            'com.umbrella.**': True,
            'com.*': False,
            'com.umb**rella.**': False,
            '???.umbrella.**': True
        }

        for pattern, expected in tests.items():
            assert glob.ModuleGlobPattern(pattern).match(name) == expected,  \
                   "failed: {}".format(pattern)
