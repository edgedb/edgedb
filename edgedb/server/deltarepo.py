##
# Copyright (c) 2008-2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.caos import proto, delta


class MetaDeltaRepository:

    def load_delta(self, id):
        raise NotImplementedError

    def delta_ref_to_id(self, ref):
        raise NotImplementedError

    def resolve_delta_ref(self, ref):
        ref = delta.DeltaRef.parse(ref)
        if not ref:
            raise delta.DeltaRefError('unknown revision: %s' % ref)
        return self.delta_ref_to_id(ref)

    def update_delta_ref(self, ref, id):
        raise NotImplementedError

    def write_delta(self, delta_obj):
        delta_set = delta.DeltaSet(deltas=[delta_obj])
        return self.write_delta_set(delta_set)

    def write_delta_set(self, delta_set):
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

    def get_meta(self, delta_obj):
        deltas = self.get_deltas(None, delta_obj.id)
        meta = proto.RealmMeta(load_builtins=False)
        deltas.apply(meta)
        return meta

    def get_meta_at(self, ref):
        delta = self.load_delta(ref)
        return self.get_meta(delta)

    def _cumulative_delta(self, ref1, ref2):
        delta = None
        v1 = self.load_delta(ref1) if ref1 else None

        if isinstance(ref2, proto.RealmMeta):
            v2 = None
            v2_meta = ref2
        else:
            v2 = self.load_delta(ref2)
            v2_meta = self.get_meta(v2)

        if v1 is not None:
            v1_meta = self.get_meta(v1)
        else:
            v1_meta = proto.RealmMeta(load_builtins=False)

        if v1 is None or v1.checksum != v2_meta.get_checksum():
            delta = v2_meta.delta(v1_meta)
        else:
            delta = None

        return v1, v1_meta, v2, v2_meta, delta

    def cumulative_delta(self, ref1, ref2):
        return self._cumulative_delta(ref1, ref2)[4]

    def calculate_delta(self, ref1, ref2, *, comment=None, preprocess=None, postprocess=None):
        v1, v1_meta, v2, v2_meta, d = self._cumulative_delta(ref1, ref2)

        if d is None and (preprocess is not None or postprocess is not None):
            d = delta.AlterRealm(module=v1_meta.main_module)

        if d is not None:
            d.preprocess = preprocess
            d.postprocess = postprocess

            parent_id = v1.id if v1 else None
            checksum = v2_meta.get_checksum()

            return delta.Delta(parent_id=parent_id, checksum=checksum,
                               comment=comment, deltas=[d],
                               formatver=delta.Delta.CURRENT_FORMAT_VERSION)
        else:
            return None
