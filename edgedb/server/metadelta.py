##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import caos
from semantix.caos import proto
from semantix.utils.datastructures import OrderedIndex
from semantix.utils import helper


def metadelta(oldmeta, newmeta):
    result = []

    for type in ('atom', 'concept', 'link'):
        new = OrderedIndex(newmeta(type, include_builtin=True), key=lambda o: o.hash())
        old = OrderedIndex(oldmeta(type, include_builtin=True), key=lambda o: o.hash())

        for hash, obj in new.items():
            i = [(obj.compare(other), other, obj) for other in old]

            if i:
                i = sorted(i, key=lambda item: item[0], reverse=True)

                if i[0][0] != 1.0:
                    # No complete match

                    mod_candidates = list(filter(lambda item: item[0] > 0.6, i))

                    if mod_candidates:
                        del old[mod_candidates[0][1]]
                        # Looks like a modified object
                        result.append(mod_candidates[0][1:])
                    else:
                        # Looks like a new object
                        result.append((None, obj))
                else:
                    del old[i[0][1]]
            else:
                result.append((None, obj))

        for hash, obj in old.items():
            result.append((obj, None))

    return result


def concept_delta(old:proto.Concept, new:proto.Concept):
    """Determine the delta between the old and the new concept object"""

    old_links = OrderedIndex(old.ownlinks.values(), key=lambda o: o.hash())
    new_links = OrderedIndex(new.ownlinks.values(), key=lambda o: o.hash())

    result = []

    for hash, link in new_links.items():
        if link in old_links:
            del old_links[link]
        else:
            similarity = [(link.compare(other), other, link) for other in old_links]

            mod_candidates = list(filter(lambda item: item[0] > 0.6, similarity))

            if mod_candidates:
                result.append(mod_candidates[0][1:])
                del old_links[mod_candidates[0][1]]
            else:
                result.append((None, link))

    for hash, link in old_links.items():
        result.append((link, None))

    return result
