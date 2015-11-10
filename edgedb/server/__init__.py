##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils import abc


class MetaBackend:
    def __init__(self, deltarepo):
        self.deltarepo = deltarepo

    def getschema(self):
        raise NotImplementedError

    def is_dirty(self):
        schema = self.getschema()
        delta = self.deltarepo.get_delta('HEAD')
        if delta:
            return schema.get_checksum() != delta.checksum
        else:
            return True

    def record_delta(self, *, preprocess=None, postprocess=None, comment=None):
        ref1 = self.deltarepo.resolve_delta_ref('HEAD')
        delta_obj = self.deltarepo.calculate_delta(ref1, self.getschema(), comment=comment,
                                                   preprocess=preprocess, postprocess=postprocess)
        if delta_obj:
            self.deltarepo.write_delta(delta_obj)
            self.deltarepo.update_delta_ref('HEAD', delta_obj.id)

    def process_delta(self, delta, schema):
        delta.apply(schema)
        return delta


class DataBackend(metaclass=abc.AbstractMeta):
    @abc.abstractmethod
    def get_session_pool(self, realm, async=False):
        pass
