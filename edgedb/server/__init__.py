##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos import proto, delta


class MetaDeltaRepository:

    def load_delta(self, id):
        raise NotImplementedError

    def resolve_delta_ref(self, ref):
        raise NotImplementedError

    def update_delta_ref(self, ref, id):
        raise NotImplementedError

    def write_delta(self, delta_obj):
        raise NotImplementedError

    def get_delta(self, id='HEAD'):
        id = self.resolve_delta_ref(id)
        if id:
            return self.load_delta(id)
        else:
            return None

    def get_deltas(self, start_rev, end_rev=None):
        start_rev = start_rev
        end_rev = end_rev or self.resolve_delta_ref('HEAD')

        return delta.DeltaSet(reversed(list(self.walk_deltas(start_rev, end_rev))))

    def walk_deltas(self, start_rev, end_rev):
        current_rev = end_rev

        while current_rev and current_rev != start_rev:
            delta = self.load_delta(current_rev)
            yield delta
            current_rev = delta.parent_id

    def calculate_delta(self, current_meta, comment=None):
        head = self.get_delta()

        meta_snapshot = proto.RealmMeta(load_builtins=False)

        if head:
            if head.checksum == current_meta.get_checksum():
                return
            else:
                deltas = self.get_deltas(None, head.id)
                deltas.apply(meta_snapshot)

        d = current_meta.delta(meta_snapshot)
        parent_id = head.id if head else None
        checksum = current_meta.get_checksum()

        delta_obj = delta.Delta(parent_id=parent_id, checksum=checksum,
                                comment=comment, deltas=[d])

        return delta_obj


class MetaBackend:
    def __init__(self, deltarepo):
        self.deltarepo = deltarepo

    def getmeta(self):
        raise NotImplementedError

    def is_dirty(self):
        meta = self.getmeta()
        delta = self.deltarepo.get_delta('HEAD')
        if delta:
            return meta.get_checksum() != delta.checksum
        else:
            return True

    def record_delta(self, comment=None):
        delta_obj = self.deltarepo.calculate_delta(self.getmeta(), comment)
        if delta_obj:
            self.deltarepo.write_delta(delta_obj)
            self.deltarepo.update_delta_ref('HEAD', delta_obj.id)


class DataBackend:
    pass
