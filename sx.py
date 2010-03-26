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

import semantix.shell
import semantix.utils.shell


def run(argv):
    return semantix.utils.shell.MainCommand.main(argv)

if __name__ == '__main__':
    sys.exit(run(sys.argv))
