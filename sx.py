#!/usr/bin/python3

##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import semantix.bootstrap

imports = {
    'required': ['semantix.shell'],
    'optional': ['semantix.config_local']
}

if __name__ == '__main__':
    sys.exit(semantix.bootstrap.run(sys.argv, imports))
