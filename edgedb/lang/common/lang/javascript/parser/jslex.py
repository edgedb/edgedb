##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import pyggy

l,tab = pyggy.getlexer("semantix/utils/lang/javascript/parser/js.pyl", debug = 1)
l.setinput("-")
while 1 :
    x = l.token()
    if x is None :
        break
    print(x, l.value, "line", tab.linenumber)
