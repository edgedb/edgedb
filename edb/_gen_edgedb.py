from __future__ import absolute_import, print_function

import os
import sys


def run():
    path = os.path.split(__file__)[0]
    name = os.path.split(sys.argv[0])[1]
    file = os.path.join(path, name)
    if os.path.isfile(file):
        os.execv(file, sys.argv)
    else:
        print("Can not execute '%s'" % name)
