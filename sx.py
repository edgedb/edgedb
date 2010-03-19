#!/usr/bin/python3

##
# Copyright (c) 2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys

import semantix.shell
import semantix.utils.shell


sys.path.insert(0, os.path.dirname(__file__))

def run(argv):
    return semantix.utils.shell.main(argv)

if __name__ == '__main__':
    sys.exit(run(sys.argv))
