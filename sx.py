#!/usr/bin/python3

##
# Copyright (c) 2010-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

os.environ['SEMANTIX_AUTO_BOOTSTRAP'] = 'no'

import metamagic.bootstrap

imports = {
    'required': ['metamagic.shell']
}

if __name__ == '__main__':
    sys.exit(metamagic.bootstrap.run(sys.argv, imports))
