##
# Copyright (c) 2012-2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


modver_cache = {}

def invalidate_modver_cache(modname=None):
    if modname is None:
        modver_cache.clear()
    else:
        modver_cache.pop(modname, None)


deptracked_modules = {}


package_tag_maps = {}
