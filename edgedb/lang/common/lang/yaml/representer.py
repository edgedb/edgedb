##
# Copyright (c) 2014 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from yaml import representer


class FrozensetRepresenter(representer.SafeRepresenter):
    def represent_frozenset(self, data):
        value = {k: None for k in data}
        return self.represent_mapping(
            'tag:metamagic.sprymix.com,2009/metamagic/frozenset', value)


representer.SafeRepresenter.add_representer(frozenset,
    FrozensetRepresenter.represent_frozenset)
